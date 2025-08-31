import socket
import struct
from .utils import Bet

def _read_bytes(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed before reading all bytes")
        buf += chunk
    return buf

def send_ack(sock, bet: Bet):
    ack_value = bet.number if bet else 0xFFFFFFFF
    ack = struct.pack('>I', ack_value)
    _send_all(sock, ack)

def _send_all(sock, data: bytes):
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket closed")
        total_sent += sent

def read_message(sock: socket.socket) -> str:
    header = _read_bytes(sock, 2)
    length = struct.unpack(">H", header)[0]
    data = _read_bytes(sock, length)
    return data.decode("utf-8")

def read_bet_batch(text: str) -> list[Bet]:
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

def send_winners(sock, available: bool, winners: list[str] = None, msg: str = ""):
    payload = ",".join(winners) if available else msg
    data = payload.encode("utf-8")
    header = struct.pack(">H", len(data))
    _send_all(sock, header)
    _send_all(sock, data)
