import socket
import logging

from common.utils import store_bets
from .protocol import read_bet_batch, send_ack

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True

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
            bets = read_bet_batch(client_sock)

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
