import socket
import logging

from common.utils import has_won, load_bets, store_bets
from .protocol import read_message, send_ack, read_bet_batch, send_winners

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self.agencies_ended = set()
        self.agencies_participated = set()
        self.winners_per_agency = {}
        self.lottery_held = False

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
        try:
            while True:
                try:
                    raw_message = read_message(client_sock)
                except ConnectionError:
                    # Cliente cerró
                    break

                if not raw_message:
                    break

                if raw_message.startswith("FIN_APUESTAS:"):
                    self._handle_finish_bet(client_sock, raw_message)

                elif raw_message.startswith("PEDIR_GANADORES:"):
                    self._handle_request_winners(client_sock, raw_message)
                    # ahora sí: después de dar ganadores, cortar loop
                    break  

                else:
                    self._handle_bet_batch(client_sock, raw_message)

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

    def _handle_finish_bet(self, client_sock, raw_message):
        agency_id = int(raw_message.split(":", 1)[1])
        self.agencies_ended.add(agency_id)
        logging.info(f'action: fin_apuestas | agency_id: {agency_id} | result: success ')
        # Disparar sorteo cuando todas las agencias que participaron hayan terminado
        if self.agencies_participated and self.agencies_participated.issubset(self.agencies_ended):
            self._draw_lottery()

    def _handle_bet_batch(self, client_sock, raw_message):
        bets = read_bet_batch(raw_message)
        success = True

        for bet in bets:
            try:
                store_bets([bet])
                logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')
                # Trackear agencias que participan
                self.agencies_participated.add(bet.agency)
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

    def _draw_lottery(self):
        try:
            logging.info('action: sorteo | result: in_progress')

            bets = load_bets()
            
            for bet in bets:
                agency_id = bet.agency

                if agency_id not in self.winners_per_agency:
                    self.winners_per_agency[agency_id] = []

                if has_won(bet):
                    self.winners_per_agency[agency_id].append(bet.document)
            
            self.lottery_held = True
            logging.info('action: sorteo | result: success')

        except Exception as e:
            logging.error(f"action: sorteo | result: fail | error: {e}")
            raise e
        
    def _handle_request_winners(self, client_sock, raw_message):
        if not self.lottery_held:
            logging.info('action: pedir_ganadores | result: fail | error: sorteo no realizado')
            send_winners(client_sock, available=False, msg="Sorteo no realizado")
            return
        agency_id = int(raw_message.split(":", 1)[1])
        winners = self.winners_per_agency.get(agency_id, [])
        
        logging.info(f"action: consulta_ganadores | result: success | agency: {agency_id} | cant_ganadores: {len(winners)}")
        send_winners(client_sock, available=True, winners=winners)

