import socket
import logging
import os
import signal
from common.utils import has_won, load_bets, store_bets
from .protocol import read_message, send_ack, read_bet_batch, send_winners

class Server:
    def __init__(self, port, listen_backlog):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._active_connections = []

        self.agencies_ended = set()
        self.agencies_participated = set()
        self.winners_per_agency = {}
        self.lottery_held = False

        self._expected_agencies = int(os.environ.get("EXPECTED_AGENCIES", "5"))

        # Registrar handler SIGTERM
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def run(self):
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                self._active_connections.append(client_sock)
            except OSError:
                break
            self.__handle_client_connection(client_sock)
            self._active_connections.remove(client_sock)

    def handle_sigterm(self, signum, frame):
        logging.info("action: shutdown | result: in_progress")
        self._running = False
        # Cerrar socket de escucha
        try:
            self._server_socket.close()
        except Exception:
            pass
        # Cerrar conexiones activas
        for conn in self._active_connections:
            try:
                conn.close()
            except Exception:
                pass
        logging.info("action: shutdown | result: success")

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def __handle_client_connection(self, client_sock):
        try:
            raw_message = read_message(client_sock)
            logging.debug(f"action: message_received | message: {raw_message}")

            if not raw_message:
                return

            if raw_message.startswith("FIN_APUESTAS:"):
                self._handle_finish_bet(client_sock, raw_message)
            elif raw_message.startswith("PEDIR_GANADORES:"):
                self._handle_request_winners(client_sock, raw_message)
            else:
                self._handle_bet_batch(client_sock, raw_message)

        except Exception as e:
            logging.error(f"action: apuesta_almacenada | result: fail | error: {e}")
        finally:
            try:
                client_sock.close()
            except Exception:
                pass

    def _handle_bet_batch(self, client_sock, raw_message):
        bets = read_bet_batch(raw_message)
        success = True
        for bet in bets:
            try:
                store_bets([bet])
                logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')
                self.agencies_participated.add(bet.agency)
            except Exception as e:
                logging.error(f"action: apuesta_almacenada | result: fail | error: {e}")
                success = False
                break

        send_ack(client_sock, bets[-1] if success else None)
        logging.info(f'action: apuesta_recibida | result: {"success" if success else "fail"} | cantidad: {len(bets)}')

    def _handle_finish_bet(self, client_sock, raw_message):
        agency_id = int(raw_message.split(":", 1)[1])
        self.agencies_ended.add(agency_id)
        logging.info(f'action: fin_apuestas | agency_id: {agency_id} | result: success')
        # Disparar sorteo si todas las agencias esperadas terminaron
        if len(self.agencies_ended) == self._expected_agencies:
            self._draw_lottery()

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
