import socket
import struct
from .utils import Bet

def _read_bytes(sock: socket.socket, n: int) -> bytes:
    """
    Lee exactamente n bytes del socket. Si se cierra el socket antes, lanza ConnectionError.
    """
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed before reading all bytes")
        buf += chunk
    return buf

def _send_all(sock: socket.socket, data: bytes):
    """
    Envía todos los bytes del buffer. Lanza ConnectionError si el socket se cierra.
    """
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket closed during send")
        total_sent += sent

def read_message(sock: socket.socket) -> str:
    """
    Lee un mensaje con prefijo de longitud de 2 bytes (>H).
    """
    header = _read_bytes(sock, 2)
    length = struct.unpack(">H", header)[0]
    data = _read_bytes(sock, length)
    return data.decode("utf-8")

def send_ack(sock: socket.socket, bet: Bet):
    """
    Envía un ACK de 4 bytes con el número de apuesta.
    Si bet es None, envía 0xFFFFFFFF
    """
    ack_value = bet.number if bet else 0xFFFFFFFF
    _send_all(sock, struct.pack(">I", ack_value))

def read_bet_batch(text: str) -> list[Bet]:
    """
    Parsea un string con múltiples apuestas separadas por ';' y retorna lista de Bet.
    """
    bets = []
    bet_strings = text.strip().split(";")
    for b in bet_strings:
        fields = b.strip().split(",")
        if len(fields) != 6:
            raise ValueError(f"Expected 6 fields per bet, got {len(fields)}")
        bets.append(Bet(
            agency=int(fields[0]),
            first_name=fields[1],
            last_name=fields[2],
            document=fields[3],
            birthdate=fields[4],
            number=int(fields[5])
        ))
    return bets

def send_winners(sock: socket.socket, available: bool, winners: list[str] = None, msg: str = ""):
    """
    Envía la lista de ganadores o un mensaje de error.
    Usa prefijo de 2 bytes (>H) para la longitud.
    """
    payload = ",".join(winners) if available else msg
    data = payload.encode("utf-8")
    _send_all(sock, struct.pack(">H", len(data)))
    _send_all(sock, data)
