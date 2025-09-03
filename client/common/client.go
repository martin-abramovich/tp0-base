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

// countBetsInFile cuenta el número total de apuestas válidas sin cargar en memoria
func countBetsInFile(filename string, agencyID string) (int, error) {
	csvFile, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %v", err)
	}
	defer csvFile.Close()

	count := 0
	scanner := bufio.NewScanner(csvFile)

	for scanner.Scan() {
		line := scanner.Text()
		if _, err := parseBetLine(line, agencyID); err == nil {
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading file: %v", err)
	}

	return count, nil
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

			// Cuando el batch está lleno o es la última apuesta, procesarlo
			if len(batch) >= batchSize || currentIndex+len(batch) >= totalBets {
				batchStart := currentIndex
				batchEnd := currentIndex + len(batch)
				isLastBatch := batchEnd >= totalBets

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
	
	// Primero contar el total de apuestas sin cargar en memoria
	totalBets, err := countBetsInFile(betsFile, c.config.ID)
	if err != nil {
		log.Errorf("action: count_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if totalBets == 0 {
		log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
		// Aún necesitamos notificar finalización y consultar ganadores aunque no haya apuestas
		if err := c.createClientSocket(); err != nil {
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
		return
	}

	if c.config.BatchMaxAmount <= 0 {
		c.config.BatchMaxAmount = 100 // Default batch size para archivos grandes
	}

	if err := c.createClientSocket(); err != nil {
		return
	}

	if err := c.sendBetsStreaming(betsFile, totalBets); err != nil {
		log.Errorf("action: send_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	if err := c.notifyEnd(); err != nil {
		log.Errorf("action: notify_end | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Consultar ganadores (manteniendo la misma conexión)
	if err := c.requestWinners(); err != nil {
		log.Errorf("action: request_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Cerrar conexión después de obtener ganadores
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	// Log de finalización explícito para que los tests detecten el evento de salida
	log.Infof("action: exit | result: success")
}

// sendBetsStreaming envía las apuestas en streaming sin cargar todo el archivo en memoria
func (c *Client) sendBetsStreaming(betsFile string, totalBets int) error {
	// Procesar las apuestas en streaming manteniendo la lógica original
	err := processBetsInBatches(betsFile, c.config.ID, totalBets, c.config.BatchMaxAmount, 
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
				// Mantener la lógica original de EOF: solo aceptar EOF si estamos en el último batch
				if err == io.EOF && isLastBatch {
					log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
					return nil // Esto terminará el procesamiento exitosamente
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

// requestWinners consulta ganadores una vez y espera la respuesta del servidor
func (c *Client) requestWinners() error {
	getMsg := fmt.Sprintf("GET_WINNERS|%s", c.config.ID)
	if err := sendTextFrame(c.conn, getMsg); err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// Leer primera respuesta
	resp, err := readTextFrame(c.conn)
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	if resp == "NOT_READY" {
		// El servidor nos mantendrá la conexión abierta y nos enviará los ganadores cuando esté listo
		// Esperamos la segunda respuesta con los ganadores
		resp, err = readTextFrame(c.conn)
		if err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}
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

	return fmt.Errorf("respuesta inesperada: %s", resp)
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