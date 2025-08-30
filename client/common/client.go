package common

import (
	"net"
	"time"
	"strconv"

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
	bet    Bet
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

func parseBetLine(line string, agencyID int) (Bet, error) {
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
func NewClient(config ClientConfig, bet Bet) *Client {
	client := &Client{
		config: config,
		stop:   make(chan struct{}),
		bet:    bet,
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
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		// Create the connection the server in every loop iteration. Send an
		select {
		case <-c.stop:
			return
		default:
			c.createClientSocket()
			
			if err := sendBet(c.conn, c.bet); err != nil {
				log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			ack, err := receiveAck(c.conn)
			if err != nil {
				log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			n, err := strconv.Atoi(c.bet.Numero)
			if ack == n {
				log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", c.bet.Documento, c.bet.Numero)
			} else {
				log.Errorf("action: apuesta_enviada | result: fail | dni: %s | numero: %s", c.bet.Documento, c.bet.Numero)
			}

			c.conn.Close()

			// Wait a time between sending one message and the next one
			select {
			case <-c.stop:
				log.Infof("action: stop_client_loop | result: success | client_id: %v", c.config.ID)
				return
			case <-time.After(c.config.LoopPeriod):
				// Continue with the loop
			}
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
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