import socket
from .utils import Bet

def _pack_uint32_big_endian(value: int) -> bytes:
    """
    Convierte un entero de 32 bits a bytes en formato big-endian.
    Equivalente a struct.pack('>I', value)
    """
    return bytes([
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF, 
        (value >> 8) & 0xFF,
        value & 0xFF
    ])

def _unpack_uint16_big_endian(data: bytes) -> int:
    """
    Convierte 2 bytes en formato big-endian a un entero de 16 bits.
    Equivalente a struct.unpack('>H', data)[0]
    """
    if len(data) != 2:
        raise ValueError("Se esperan exactamente 2 bytes")
    return (data[0] << 8) | data[1]

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
    ack = _pack_uint32_big_endian(bet.number)
    _send_all(sock, ack)

def _send_all(sock, data: bytes):
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket closed")
        total_sent += sent

def read_bet(sock: socket.socket) -> Bet:
    header = _read_bytes(sock, 2)
    if not header:
        raise EOFError("Socket closed")
    
    length = _unpack_uint16_big_endian(header)
    data = _read_bytes(sock, length)
    text = data.decode("utf-8")

    fields = text.strip().split(",")
    if len(fields) != 6:
        raise ValueError(f"Expected 6 fields, got {len(fields)}")

    return Bet(
        agency=int(fields[0]),
        first_name=fields[1],
        last_name=fields[2],
        document=fields[3],
        birthdate=fields[4],
        number=int(fields[5])
    )