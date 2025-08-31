package common

import (
	"net"
	"time"
	"strconv"
	"os"
	"bufio"
	"fmt"
	"strings"
	"io"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMaxAmount   int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
	stop   chan struct{}
}

func readBetsFromFile(filename string, agencyID string) ([]Bet, error) {
	csvFile, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer csvFile.Close()

	var bets []Bet
	scanner := bufio.NewScanner(csvFile)

	for scanner.Scan() {
        line := scanner.Text()
        if bet, err := parseBetLine(line, agencyID); err == nil {
            bets = append(bets, bet)
        } else {
            log.Warningf("Error parsing line '%s': %v", line, err)
        }
    }
    
    return bets, nil
}

func parseBetLine(line string, agencyID string) (Bet, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Bet{}, fmt.Errorf("empty line")
	}
	
	fields := strings.Split(line, ",")
	if len(fields) != 5 {
		return Bet{}, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}

	return Bet{
		Agencia: agencyID,
		Nombre: fields[0],
		Apellido: fields[1],
		Documento: fields[2],
		Nacimiento: fields[3],
		Numero: fields[4],
	}, nil
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
		stop:   make(chan struct{}),
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

// StartClientLoop sends all bets and then notifies completion
func (c *Client) StartClientLoop() {
	betsFile := fmt.Sprintf("/agency-%s.csv", c.config.ID)
	bets, err := readBetsFromFile(betsFile, c.config.ID)
	if err != nil {
		log.Errorf("action: read_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if len(bets) == 0 {
		log.Infof("action: no_bets_to_send | client_id: %v", c.config.ID)
		// Aún así enviar FIN_APUESTAS para indicar que esta agencia "terminó"
		if err := c.sendFinApuestas(); err != nil {
			log.Errorf("action: fin_apuestas | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}
		if err := c.requestWinnersWithRetry(); err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}
		return
	}

	if c.config.BatchMaxAmount <= 0 {
		c.config.BatchMaxAmount = len(bets)
	}

	// Fase 1: Enviar todas las apuestas
	allBetsSent := c.sendAllBets(bets)
	
	log.Infof("action: all_batches_processed | client_id: %v | bets_sent: %v", c.config.ID, allBetsSent)

	// Fase 2: Enviar notificación FIN_APUESTAS (siempre, incluso si hubo errores)
	if err := c.sendFinApuestas(); err != nil {
		log.Errorf("action: fin_apuestas | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Fase 3: Solicitar ganadores con reintentos
	if err := c.requestWinnersWithRetry(); err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}

// sendAllBets envía todas las apuestas en batches usando una conexión dedicada
func (c *Client) sendAllBets(bets []Bet) bool {
	if err := c.createClientSocket(); err != nil {
		return false
	}
	defer c.conn.Close()

	allBetsSent := true
	for i := 0; i < len(bets); i += c.config.BatchMaxAmount {
		end := i + c.config.BatchMaxAmount
		if end > len(bets) {
			end = len(bets)
		}
		batch := bets[i:end]

		if err := sendBetBatch(c.conn, batch); err != nil {
			log.Errorf("action: send_bet_batch | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
			allBetsSent = false
			break
		}

		ack, err := receiveAck(c.conn)
		last := batch[len(batch)-1]
		if err != nil {
			log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
			// No marcar como error crítico si es el último batch
			if end < len(bets) {
				allBetsSent = false
			}
			continue
		}

		log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", last.Documento, last.Numero)

		n, err := strconv.Atoi(last.Numero)
		if err == nil && ack == n {
			log.Infof("action: apuestas_enviadas | result: success")
		} else {
			log.Errorf("action: apuestas_enviadas | result: fail")
		}
	}
	return allBetsSent
}

// sendFinApuestas envía la notificación FIN_APUESTAS usando una conexión nueva
func (c *Client) sendFinApuestas() error {
	if err := c.createClientSocket(); err != nil {
		return err
	}
	defer c.conn.Close()

	log.Infof("action: sending_fin_apuestas | client_id: %v", c.config.ID)

	if err := sendNotification(c.conn, c.config.ID); err != nil {
		return fmt.Errorf("error sending FIN_APUESTAS: %v", err)
	}

	log.Infof("action: fin_apuestas | result: success | client_id: %v", c.config.ID)
	return nil
}

// requestWinnersWithRetry solicita ganadores con reintentos usando conexiones nuevas
func (c *Client) requestWinnersWithRetry() error {
	const maxRetries = 10
	const retryDelay = 1 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Infof("action: request_winners | attempt: %d | client_id: %v", attempt, c.config.ID)
		
		// Crear conexión nueva para cada intento
		if err := c.createClientSocket(); err != nil {
			log.Errorf("action: request_winners | result: fail | attempt: %d | client_id: %v | error: %v",
				attempt, c.config.ID, err)
			time.Sleep(retryDelay)
			continue
		}

		winners, err := requestWinners(c.conn, c.config.ID)
		c.conn.Close()

		if err != nil {
			log.Infof("action: request_winners | result: retry | attempt: %d | client_id: %v | error: %v",
				attempt, c.config.ID, err)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				continue
			}
			return fmt.Errorf("failed after %d attempts: %v", maxRetries, err)
		}

		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winners))
		return nil
	}

	return fmt.Errorf("exceeded maximum retries (%d)", maxRetries)
}

// StopClientLoop Stops the client loop
func (c *Client) StopClientLoop() {
	close(c.stop)
	log.Infof("action: stop_client_loop | result: success | client_id: %v", c.config.ID)
	if c.conn != nil {
		c.conn.Close()
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}
}