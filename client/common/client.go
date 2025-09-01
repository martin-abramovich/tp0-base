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

func (c *Client) StartClientLoop() {
	select {
	case <-c.stop:
		return
	default:
		// Continuar con la ejecución
	}

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

	if err := c.sendBets(bets); err != nil {
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

	// Pequeño delay para dar tiempo a que el agregador de logs entregue
	// la línea de consulta antes de cortar por cantidad de 'exit'
	time.Sleep(2 * time.Second)
	// Log de finalización explícito para que los tests detecten el evento de salida
	log.Infof("action: exit | result: success")
}

// sendBets envía todas las apuestas en lotes
func (c *Client) sendBets(bets []Bet) error {
	for i := 0; i < len(bets); i += c.config.BatchMaxAmount {
		select {
		case <-c.stop:
			return fmt.Errorf("client stopped")
		default:
			// Continuar con la ejecución
		}

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
			return err
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
			return err
		}

		log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", last.Documento, last.Numero)

		n, err := strconv.Atoi(last.Numero)
		if err == nil && ack == n {
			log.Infof("action: apuestas_enviadas | result: success")
		} else {
			log.Errorf("action: apuestas_enviadas | result: fail")
		}
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