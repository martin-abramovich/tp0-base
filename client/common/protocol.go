package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
)

func writeAll(conn net.Conn, data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		n, err := conn.Write(data[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}

func readAll(conn net.Conn, buf []byte) error {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := conn.Read(buf[totalRead:])
		if err != nil {
			if err == io.EOF && totalRead > 0 {
				return fmt.Errorf("unexpected EOF, read %d bytes of %d", totalRead, len(buf))
			}
			return err
		}
		totalRead += n
	}
	return nil
}

func receiveAck(conn net.Conn) (int, error) {
	buf := make([]byte, 4)
	if err := readAll(conn, buf); err != nil {
		return 0, fmt.Errorf("error reading ACK: %w", err)
	}

	ackNumber := int(binary.BigEndian.Uint32(buf))
	return ackNumber, nil
}

func sendTextFrame(conn net.Conn, text string) error {
    data := []byte(text)
    if len(data) > 0xFFFF {
        return fmt.Errorf("frame too large: %d", len(data))
    }
    header := make([]byte, 2)
    binary.BigEndian.PutUint16(header, uint16(len(data)))
    if err := writeAll(conn, header); err != nil {
        return fmt.Errorf("error sending header: %v", err)
    }
    if err := writeAll(conn, data); err != nil {
        return fmt.Errorf("error sending payload: %v", err)
    }
    return nil
}

func readTextFrame(conn net.Conn) (string, error) {
    header := make([]byte, 2)
    if err := readAll(conn, header); err != nil {
        return "", err
    }
    length := binary.BigEndian.Uint16(header)
    if length == 0 {
        return "", nil
    }
    buf := make([]byte, length)
    if err := readAll(conn, buf); err != nil {
        return "", err
    }
    return string(buf), nil
}

func sendBet(conn net.Conn, bet Bet) error {
	payload := fmt.Sprintf("%s,%s,%s,%s,%s,%s",
		bet.Agencia,
		bet.Nombre,
		bet.Apellido,
		bet.Documento,
		bet.Nacimiento,
		bet.Numero,
	)
	data := []byte(payload)
	length := uint16(len(data))

	header := make([]byte, 2)
	binary.BigEndian.PutUint16(header, length)

	// Primero enviar header
	if err := writeAll(conn, header); err != nil {
		return fmt.Errorf("error sending header: %v", err)
	}

	// Luego enviar payload
	if err := writeAll(conn, data); err != nil {
		return fmt.Errorf("error sending payload: %v", err)
	}

	return nil
}


func sendBetBatch(conn net.Conn, bets []Bet) error {
	var payloads []string
	for _, bet := range bets {
		betPayload := fmt.Sprintf("%s,%s,%s,%s,%s,%s",
			bet.Agencia,
			bet.Nombre,
			bet.Apellido,
			bet.Documento,
			bet.Nacimiento,
			bet.Numero,
		)
		payloads = append(payloads, betPayload)
	}

	payload := strings.Join(payloads, ";")
	data := []byte(payload)
	
	length := uint16(len(data))
	header := make([]byte, 2)
	binary.BigEndian.PutUint16(header, length)

	if err := writeAll(conn, header); err != nil {
		return fmt.Errorf("error sending header: %v", err)
	}

	if err := writeAll(conn, data); err != nil {
		return fmt.Errorf("error sending payload: %v", err)
	}

	return nil
}