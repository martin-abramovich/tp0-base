import socket
import struct
from .utils import Bet

def _read_bytes(sock: socket.socket, n: int) -> bytes:
    """
    Lee un número específico de bytes del socket.
    Si el socket está cerrado, lanza un EOFError.
    Si no se puede leer el número de bytes especificado, lanza un ValueError.
    Si se lee correctamente, devuelve los bytes leídos.
    """
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed before reading all bytes")
        buf += chunk
    return buf

def send_ack(sock, bet: Bet):
    if bet is None:
        # Usar 0xFFFFFFFF (uint32) como código de error
        ack_value = 0xFFFFFFFF
    else:
        ack_value = bet.number
    ack = struct.pack('>I', ack_value)
    _send_all(sock, ack)

def _send_all(sock, data: bytes):
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket closed")
        total_sent += sent

def read_bet_batch(sock: socket.socket) -> list[Bet]:
    header = _read_bytes(sock, 2)
    if not header:
        raise EOFError("Socket closed")
    
    length = struct.unpack(">H", header)[0]
    data = _read_bytes(sock, length)
    text = data.decode("utf-8")

    bet_strings = text.strip().split(";")
    bets = []
    for bet_string in bet_strings:
        fields = bet_string.strip().split(",")
        if len(fields) != 6:
            raise ValueError(f"Expected 6 fields, got {len(fields)}")
        bets.append(Bet(
            agency=int(fields[0]),
            first_name=fields[1],
            last_name=fields[2],
            document=fields[3],
            birthdate=fields[4],
            number=int(fields[5])
        ))
    return bets



