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

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMaxAmount   int
}

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

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
		stop:   make(chan struct{}),
	}
	return client
}

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

func (c *Client) StartClientLoop() {
	select {
	case <-c.stop:
		return
	}
	default:
	
		betsFile := fmt.Sprintf("/agency-%s.csv", c.config.ID)
		bets, err := readBetsFromFile(betsFile, c.config.ID)
		if err != nil {
			log.Errorf("action: read_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		if c.config.BatchMaxAmount <= 0 {
			c.config.BatchMaxAmount = len(bets)
		}

		if err := c.createClientSocket(); err != nil {
			return
		}


		c.sendBets(bets)
		log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)

		c.notifyEnd()
		// Cerrar la conexión para liberar al servidor de este cliente
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		log.Infof("action: fin_envio | result: success | client_id: %v", c.config.ID)

		c.getWinners()

		// Pequeño delay para dar tiempo a que el agregador de logs entregue
		// la línea de consulta antes de cortar por cantidad de 'exit'
		time.Sleep(2 * time.Second)
		// Log de finalización explícito para que los tests detecten el evento de salida
		log.Infof("action: exit | result: success")
}

func (c *Client) sendBets(bets []Bet) {
	for i := 0; i < len(bets); i += c.config.BatchMaxAmount {
		end := i + c.config.BatchMaxAmount
		if end > len(bets) {
			end = len(bets)
		}
		batch := bets[i:end]

		if err := sendBetBatch(c.conn, batch); err != nil {
			log.Errorf("action: send_bet_batch | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		ack, err := receiveAck(c.conn)
		last := batch[len(batch)-1]
		if err != nil {
			if err == io.EOF && end == len(bets) {
				log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
				break
			}
			log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", last.Documento, last.Numero)

		n, err := strconv.Atoi(last.Numero)
		if err == nil && ack == n {
			log.Infof("action: apuestas_enviadas | result: success")
		} else {
			log.Errorf("action: apuestas_enviadas | result: fail")
		}
	}
}

func (c *Client) notifyEnd() {
	// Enviar fin de envío sobre la conexión existente
	if c.conn == nil {
		log.Errorf("action: fin_envio | result: fail | client_id: %v | error: %v", c.config.ID, "no active connection")
		return
	}
	endMsg := fmt.Sprintf("END|%s", c.config.ID)
	if err := sendTextFrame(c.conn, endMsg); err != nil {
		log.Errorf("action: fin_envio | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}
}

func (c *Client) getWinners() {
	for attempts := 0; attempts < 10; attempts++ {
		// abrir nueva conexión por intento
		if err := c.createClientSocket(); err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		conn := c.conn

		getMsg := fmt.Sprintf("GET_WINNERS|%s", c.config.ID)
		if err := sendTextFrame(conn, getMsg); err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			conn.Close()
			c.conn = nil
			time.Sleep(500 * time.Millisecond)
			continue
		}
		resp, err := readTextFrame(conn)
		if err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			conn.Close()
			c.conn = nil
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// cerrar siempre la conexión de consulta
		conn.Close()
		c.conn = nil

		if resp == "NOT_READY" {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if strings.HasPrefix(resp, "WINNERS|") {
			list := strings.TrimPrefix(resp, "WINNERS|")
			cant := 0
			if strings.TrimSpace(list) != "" {
				parts := strings.Split(list, ",")
				for _, p := range parts {
					if strings.TrimSpace(p) != "" {
						cant++
					}
				}
			}
			log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", cant)
			return
		}
		// Respuesta inesperada: reintentar
		time.Sleep(500 * time.Millisecond)
	}
	log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, "max retries exceeded")
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