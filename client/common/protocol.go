package common

import (
	"encoding/binary"
	"fmt"
	"net"
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

func sendBet(conn net.Conn, bet Bet) error {
	payload := fmt.Sprintf("%d,%s,%s,%s,%s,%d",
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
