import socket
import logging

from common.utils import store_bets, load_bets, has_won
from .protocol import read_bet_batch, send_ack, read_frame_text, parse_bet_batch_text, send_text_frame

class Server:
    def __init__(self, port, listen_backlog, expected_agencies):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        # Estado del sorteo
        self._finished_agencies: set[int] = set()
        self._lottery_done: bool = False
        # Cantidad de agencias esperadas
        self.expected_agencies = expected_agencies

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server

        while self._running:
            try:
                client_sock = self.__accept_new_connection()
            except OSError:
                break
            self.__handle_client_connection(client_sock)

    def handle_sigterm(self, signum, frame):
        """
        Handle signal to graceful shutdown the server
        """
        logging.info('action: shutdown | result: in_progress')
        self._server_socket.close()
        self._running = False
        logging.info('action: shutdown | result: success')

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            while True:
                try:
                    text = read_frame_text(client_sock)
                except ConnectionError:
                    # Client closed connection
                    break

                # Intentar parsear como batch de apuestas; si falla, es un comando
                bets = None
                try:
                    bets = parse_bet_batch_text(text)
                except Exception:
                    bets = None

                if bets is not None and len(bets) > 0:
                    success = True

                    for bet in bets:
                        try:
                            store_bets([bet])
                            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')
                        except Exception as e:
                            logging.error(f"action: apuesta_almacenada | result: fail | error: {e}")
                            success = False
                            break

                    if success:
                        logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
                        send_ack(client_sock, bets[-1])
                    else:
                        logging.info(f'action: apuesta_recibida | result: fail | cantidad: {len(bets)}')
                        send_ack(client_sock, None)
                    continue

                # Comandos
                if text.startswith('END|'):
                    try:
                        agency_id = int(text.split('|', 1)[1])
                        self._finished_agencies.add(agency_id)
                        if not self._lottery_done and self.expected_agencies == len(self._finished_agencies):
                            self._lottery_done = True
                            logging.info('action: sorteo | result: success')
                    except Exception as e:
                        logging.error(f"action: end_notify | result: fail | error: {e}")
                    # No es necesario enviar respuesta para END
                    continue

                if text.startswith('GET_WINNERS|'):
                    try:
                        agency_id = int(text.split('|', 1)[1])
                    except Exception:
                        send_text_frame(client_sock, 'NOT_READY')
                        continue

                    if not self._lottery_done:
                        send_text_frame(client_sock, 'NOT_READY')
                        continue

                    # Calcular ganadores para la agencia
                    winners: list[str] = []
                    try:
                        for bet in load_bets():
                            if bet.agency == agency_id and has_won(bet):
                                winners.append(bet.document)
                    except Exception as e:
                        logging.error(f"action: consulta_ganadores | result: fail | error: {e}")
                        send_text_frame(client_sock, 'WINNERS|')
                        continue

                    payload = 'WINNERS|' + (','.join(winners))
                    send_text_frame(client_sock, payload)
                    continue

                # Mensaje desconocido
                logging.warning(f"action: mensaje_desconocido | result: fail | payload: {text}")

        except OSError as e:
            logging.error(f"action: apuesta_almacenada | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
