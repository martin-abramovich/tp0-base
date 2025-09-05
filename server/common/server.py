import socket
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from common.thread_safe_storage import storage
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
        # Cantidad esperada de agencias para realizar el sorteo
        self._expected_agencies: int = expected_agencies
        
        # Locks para sincronización
        self._state_lock = threading.RLock()  # Para estado del sorteo
        self._executor = None  # Se inicializa en run()

    def run(self):
        """
        Server loop with ThreadPoolExecutor for parallel client handling

        Server that accepts new connections and processes messages in parallel.
        Each client connection is handled in a separate thread from the pool.
        """
        
        # Usar ThreadPoolExecutor para manejar clientes en paralelo
        # max_workers=20 permite hasta 20 conexiones simultáneas
        with ThreadPoolExecutor(max_workers=20, thread_name_prefix="ClientHandler") as executor:
            self._executor = executor
            logging.info('action: threadpool_initialized | result: success | max_workers: 20')
            
            while self._running:
                try:
                    client_sock = self.__accept_new_connection()
                    # Enviar manejo del cliente a un thread del pool
                    executor.submit(self.__handle_client_connection, client_sock)
                except OSError:
                    logging.info('action: server_shutdown | result: in_progress')
                    break
            
            logging.info('action: waiting_for_threads | result: in_progress')
            # El context manager se encarga de esperar que terminen todos los threads

    def handle_sigterm(self, signum, frame):
        """
        Handle signal to graceful shutdown the server
        """
        logging.info('action: shutdown | result: in_progress')
        
        # Marcar que el servidor debe detenerse
        self._running = False
        
        # Cerrar socket del servidor para detener accept()
        try:
            self._server_socket.close()
        except:
            pass
            
        # No hay conexiones pendientes que cerrar con el nuevo protocolo
        
        # Notificar al executor que debe hacer shutdown
        if self._executor:
            logging.info('action: shutting_down_executor | result: in_progress')
            self._executor.shutdown(wait=True)
            logging.info('action: executor_shutdown | result: success')
        
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
                            storage.store_bets([bet])
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
                        with self._state_lock:
                            self._finished_agencies.add(agency_id)
                            if not self._lottery_done and len(self._finished_agencies) >= self._expected_agencies:
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
                        break

                    # Verificar si el sorteo ya está listo
                    lottery_done = False
                    with self._state_lock:
                        lottery_done = self._lottery_done
                    
                    if not lottery_done:
                        # Sorteo no listo, responder NOT_READY y mantener conexión abierta
                        send_text_frame(client_sock, 'NOT_READY')
                        # NO cerrar la conexión, mantenerla abierta para el siguiente intento
                        continue  # Continuar el loop para el siguiente mensaje
                    else:
                        # Sorteo ya realizado, enviar ganadores inmediatamente
                        self._send_winners_to_agency(client_sock, agency_id)
                        break  # Terminar después de enviar ganadores

                # Mensaje desconocido
                logging.warning(f"action: mensaje_desconocido | result: fail | payload: {text}")

        except Exception as e:
            logging.error(f"action: connection_error | result: fail | error: {e}")
        finally:
            # Cerrar conexión siempre
            try:
                client_sock.close()
            except:
                pass

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

    def _send_winners_to_agency(self, client_sock, agency_id):
        """
        Envía los ganadores de una agencia específica
        """
        winners: list[str] = []
        try:
            for bet in storage.load_bets():
                if bet.agency == agency_id and storage.has_won(bet):
                    winners.append(bet.document)
        except Exception as e:
            logging.error(f"action: consulta_ganadores | result: fail | error: {e}")
            send_text_frame(client_sock, 'WINNERS|')
            return

        payload = 'WINNERS|' + (','.join(winners))
        send_text_frame(client_sock, payload)