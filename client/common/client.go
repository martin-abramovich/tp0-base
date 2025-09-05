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


// processBetsInBatches procesa las apuestas en batches sin cargar todo el archivo en memoria
// Mantiene los índices originales para preservar la lógica de BatchMaxAmount y EOF
func processBetsInBatches(filename string, agencyID string, totalBets int, batchSize int, processor func([]Bet, int, int, bool) error) error {
	csvFile, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer csvFile.Close()

	scanner := bufio.NewScanner(csvFile)
	batch := make([]Bet, 0, batchSize)
	currentIndex := 0

	for scanner.Scan() {
		line := scanner.Text()
		if bet, err := parseBetLine(line, agencyID); err == nil {
			batch = append(batch, bet)

			// Cuando el batch está lleno, procesarlo
			if len(batch) >= batchSize {
				batchStart := currentIndex
				batchEnd := currentIndex + len(batch)
				isLastBatch := false

				if err := processor(batch, batchStart, batchEnd, isLastBatch); err != nil {
					return err
				}

				currentIndex = batchEnd
				batch = batch[:0] // Limpiar el batch reutilizando memoria
			}
		} else {
			log.Warningf("Error parsing line '%s': %v", line, err)
		}
	}

	// Procesar último batch si quedó algo
	if len(batch) > 0 {
		batchStart := currentIndex
		batchEnd := currentIndex + len(batch)
		isLastBatch := true

		if err := processor(batch, batchStart, batchEnd, isLastBatch); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	return nil
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

func (c *Client) StartClientLoop() {
	select {
	case <-c.stop:
		return
	default:
		// Continuar con la ejecución
	}

	betsFile := fmt.Sprintf("/agency-%s.csv", c.config.ID)
	
	if c.config.BatchMaxAmount <= 0 {
		c.config.BatchMaxAmount = 100 // Default batch size para archivos grandes
	}

	if err := c.createClientSocket(); err != nil {
		return
	}

	if err := c.sendBetsStreaming(betsFile); err != nil {
		log.Errorf("action: send_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if err := c.notifyEnd(); err != nil {
		log.Errorf("action: notify_end | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if err := c.requestWinners(); err != nil {
		log.Errorf("action: request_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	log.Infof("action: exit | result: success")
}

// sendBetsStreaming envía las apuestas en streaming sin cargar todo el archivo en memoria
func (c *Client) sendBetsStreaming(betsFile string) error {
	err := processBetsInBatches(betsFile, c.config.ID, -1, c.config.BatchMaxAmount, 
		func(batch []Bet, batchStart, batchEnd int, isLastBatch bool) error {
			select {
			case <-c.stop:
				return fmt.Errorf("client stopped")
			default:
				// Continuar con la ejecución
			}

			if err := sendBetBatch(c.conn, batch); err != nil {
				log.Errorf("action: send_bet_batch | result: fail | client_id: %v | error: %v",
					c.config.ID, err)
				return err
			}

			ack, err := receiveAck(c.conn)
			last := batch[len(batch)-1]
			if err != nil {
				if err == io.EOF {
					log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
					return nil
				}
				log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v",
					c.config.ID, err)
				return err
			}

			log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", last.Documento, last.Numero)

			n, err := strconv.Atoi(last.Numero)
			if err == nil && ack == n {
				log.Infof("action: apuestas_enviadas | result: success")
			} else {
				log.Errorf("action: apuestas_enviadas | result: fail")
			}
			return nil
		})

	if err != nil {
		return err
	}

	log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
	return nil
}

// notifyEnd notifica al servidor que se terminó el envío de apuestas
func (c *Client) notifyEnd() error {
	endMsg := fmt.Sprintf("END|%s", c.config.ID)
	if err := sendTextFrame(c.conn, endMsg); err != nil {
		log.Errorf("action: fin_envio | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	return nil
}

// requestWinners consulta ganadores usando polling hasta que estén listos
// Usa la misma conexión que ya está abierta
func (c *Client) requestWinners() error {
	maxRetries := 30 // Máximo 30 intentos (30 segundos con 1 segundo de intervalo)
	retryInterval := 1 * time.Second
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		if c.conn == nil {
			return fmt.Errorf("connection is closed")
		}

		getMsg := fmt.Sprintf("GET_WINNERS|%s", c.config.ID)
		if err := sendTextFrame(c.conn, getMsg); err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}

		// Leer respuesta
		resp, err := readTextFrame(c.conn)
		if err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}

		if resp == "NOT_READY" {
			log.Infof("action: consulta_ganadores | result: in_progress | client_id: %v | attempt: %d", c.config.ID, attempt+1)
			time.Sleep(retryInterval)
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
			return nil
		}

		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | unexpected_response: %s", c.config.ID, resp)
		time.Sleep(retryInterval)
	}

	return fmt.Errorf("timeout waiting for winners after %d attempts", maxRetries)
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