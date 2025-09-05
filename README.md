### Correcciones

Se realizaron las siguientes correcciones de implementación:

1. **Eliminación de la función `countBetsInFile`**: Se removió la función `countBetsInFile` del archivo `client.go` ya que no agregaba valor. Se simplificó la función `StartClientLoop` para que no requiera contar las apuestas previamente.

2. **Corrección del manejo de SIGTERM**: Se corrigió el manejo de SIGTERM para que notifique correctamente a los workers del `ThreadPoolExecutor` y realice un graceful shutdown. 

3. **Reemplazo de conexiones pendientes por protocolo de sincronización**: Se eliminó completamente el sistema de conexiones pendientes (`_pending_winners_requests`) y se implementó un protocolo de polling. El cliente hace polling periódico hasta que los ganadores estén listos.

### Ejercicio 8

Implementé que el servidor ahora maneje conexiones y procese mensajes en paralelo utilizando multithreading. Implementé un `ThreadPoolExecutor` con hasta 20 workers que permite atender múltiples clientes simultáneamente. La arquitectura funciona con un thread principal que acepta nuevas conexiones mientras que los threads workers del pool se encargan de procesar los mensajes de cada cliente de forma independiente y paralela ejecutando `handle_client_connection`.

Para garantizar la consistencia de datos usé mecanismos de sincronización. El estado del sorteo está protegido por `_state_lock` (un RLock que maneja `_finished_agencies` y `_lottery_done`), mientras que las conexiones pendientes están protegidas por `_pending_lock` para el diccionario `_pending_winners_requests`. Además, implementé `ThreadSafeStorage` como wrapper con RLock para todas las operaciones de almacenamiento, asegurando que múltiples threads puedan acceder al storage sin corromper los datos.

#### Sincronización

1. **Lock de estado (`_state_lock`)**: Protege variables compartidas del sorteo:
   - `_finished_agencies`: Set de agencias que terminaron
   - `_lottery_done`: Flag del estado del sorteo

2. **Lock de conexiones pendientes (`_pending_lock`)**: Protege el diccionario de consultas en espera:
   - `_pending_winners_requests`: Diccionario que mapea agency_id -> client_socket

3. **ThreadSafeStorage**: Wrapper thread-safe para operaciones de almacenamiento que no son thread-safe:
   - `store_bets()`: Escritura de apuestas al CSV
   - `load_bets()`: Lectura de apuestas desde CSV
   - `has_won()`: Verificación de ganadores

#### Snippets importantes del código:

##### Flujo:
```python
# server/common/server.py - Loop principal
def run(self):
    with ThreadPoolExecutor(max_workers=20, thread_name_prefix="ClientHandler") as executor:
        self._executor = executor
        logging.info('action: threadpool_initialized | result: success | max_workers: 20')
        
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                # Delegar cada cliente a un thread del pool
                executor.submit(self.__handle_client_connection, client_sock)
            except OSError:
                logging.info('action: server_shutdown | result: in_progress')
                break
```

##### Sincronización con ThreadSafeStorage:
```python
# server/common/thread_safe_storage.py - Wrapper thread-safe
class ThreadSafeStorage:
    def __init__(self):
        self.lock = threading.RLock()

    def store_bets(self, bets):
        with self.lock:
            return store_bets(bets)
    
    def load_bets(self):
        with self.lock:
            return load_bets()
```

##### Sincronización con Locks:
```python
# server/common/server.py - Protección de variables compartidas
def __handle_client_connection(self, client_sock):
    if text.startswith('END|'):
        agency_id = int(text.split('|', 1)[1])
        with self._state_lock:
            self._finished_agencies.add(agency_id)
            if not self._lottery_done and len(self._finished_agencies) >= self._expected_agencies:
                self._lottery_done = True
                logging.info('action: sorteo | result: success')
                self._notify_pending_winners()  # Responder consultas en espera
    
    elif text.startswith('GET_WINNERS|'):
        agency_id = int(text.split('|', 1)[1])
        
        # Usar locks separados para evitar deadlock
        lottery_done = False
        with self._state_lock:
            lottery_done = self._lottery_done
        
        if not lottery_done:
            # Agregar a lista de espera de forma thread-safe
            with self._pending_lock:
                self._pending_winners_requests[agency_id] = client_sock
            send_text_frame(client_sock, 'NOT_READY')
            return  # Mantener conexión abierta
        else:
            # Responder inmediatamente
            self._send_winners_to_agency(client_sock, agency_id)
```

#### Cómo ejecutar el ejercicio

1. **Generar el archivo Docker Compose con múltiples clientes:**
   ```bash
   ./generar-compose.sh docker-compose-dev.yaml 5
   ```

2. **Descomprimir los archivos en la carpeta .data `agency-{ID}.csv`**

3. **Levantar el sistema:**
   ```bash
   make docker-compose-up
   ```

4. **Ver los logs:**
   ```bash
   make docker-compose-logs
   ```

4. **Detener el sistema:**
   ```bash
   make docker-compose-down
   ```

### Ejercicio 7

- **Cliente:** Después de enviar todas las apuestas, cada cliente notifica al servidor que terminó. Una vez completado el sorteo, los clientes pueden consultar la lista de ganadores específicos de su agencia. El sistema maneja consultas tempranas manteniendo conexiones activas hasta que el sorteo esté disponible.

Agregué dos nuevos comandos: **`END|{agency_id}`** para notificar finalización y **`GET_WINNERS|{agency_id}`** para consultar ganadores.

- **Servidor**: Registra las agencias finalizadas en `_finished_agencies`, y solo ejecuta el sorteo cuando `_finished_agencies` es igual a `EXPECTED_AGENCIES` que se pasa como variable de entorno. Caso contrario, el default configurado en server/config.ini es 5.

El servidor utiliza las funciones `load_bets()` y `has_won()` para determinar ganadores y responde con **`WINNERS|{lista_dni}`** conteniendo únicamente los DNI ganadores de cada agencia. Si el sorteo aún no fue realizado, se envía `NOT_READY`, se agrega a pending_winners_requests que es un diccionario que guarda las conexiones de clientes que están esperando los resultados del sorteo. La clave (int) es el ID de la agencia y el valor (object) es el socket de conexión del cliente. Esto me evitó que los clientes tengas que hacer polling hasta obtener los resultados.

Además. se reutiliza la misma conexión TCP para envío de apuestas, notificación de fin y consulta de ganadores.

#### Snippets importantes del código:

##### Notificación de finalización:
```go
// client/common/client.go - Secuencia completa del cliente
func (c *Client) StartClientLoop() {
    if err := c.sendBetsStreaming(betsFile, totalBets); err != nil {
        log.Errorf("action: send_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return
    }
    
    if err := c.notifyEnd(); err != nil {
        log.Errorf("action: notify_end | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return
    }
    
    if err := c.requestWinners(); err != nil {
        log.Errorf("action: request_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return
    }
}

func (c *Client) notifyEnd() error {
    endMsg := fmt.Sprintf("END|%s", c.config.ID)
    if err := sendTextFrame(c.conn, endMsg); err != nil {
        log.Errorf("action: fin_envio | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return err
    }
    return nil
}
```

##### Consulta de Ganadores - Cliente:
```go
// client/common/client.go - Manejo de consulta de ganadores
func (c *Client) requestWinners() error {
    getMsg := fmt.Sprintf("GET_WINNERS|%s", c.config.ID)
    if err := sendTextFrame(c.conn, getMsg); err != nil {
        log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return err
    }

    // Leer primera respuesta
    resp, err := readTextFrame(c.conn)
    if err != nil {
        log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return err
    }

    if resp == "NOT_READY" {
        // El servidor nos mantendrá la conexión abierta y nos enviará los ganadores cuando esté listo
        resp, err = readTextFrame(c.conn)
        if err != nil {
            log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
            return err
        }
    }

    if strings.HasPrefix(resp, "WINNERS|") {
        list := strings.TrimPrefix(resp, "WINNERS|")
        cant := 0
        if strings.TrimSpace(list) != "" {
            parts := strings.Split(list, ",")
            for _, p := range parts {
                if strings.TrimSpace(p) != "" {
                    cant++
                }
            }
        }
        log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", cant)
        return nil
    }

    return fmt.Errorf("respuesta inesperada: %s", resp)
}
```

##### Sorteo:
```python
# server/common/server.py - Manejo del sorteo y consultas
def __handle_client_connection(self, client_sock):
    if text.startswith('END|'):
        agency_id = int(text.split('|', 1)[1])
        with self._state_lock:
            self._finished_agencies.add(agency_id)
            if not self._lottery_done and len(self._finished_agencies) >= self._expected_agencies:
                self._lottery_done = True
                logging.info('action: sorteo | result: success')
                self._notify_pending_winners()  # Responder consultas en espera
    
    elif text.startswith('GET_WINNERS|'):
        agency_id = int(text.split('|', 1)[1])
        
        # Usar locks separados para evitar deadlock
        lottery_done = False
        with self._state_lock:
            lottery_done = self._lottery_done
        
        if not lottery_done:
            # Guardar conexión para notificar cuando el sorteo esté listo
            with self._pending_lock:
                self._pending_winners_requests[agency_id] = client_sock
            send_text_frame(client_sock, 'NOT_READY')
            return  # NO cerrar la conexión, mantenerla abierta
        else:
            # Sorteo ya realizado, enviar ganadores inmediatamente
            self._send_winners_to_agency(client_sock, agency_id)
            break  # Terminar después de enviar ganadores
```

##### Enviar ganadores sin broadcast:
```python
# server/common/server.py - Filtrado de ganadores por agencia
def _send_winners_to_agency(self, client_sock, agency_id):
    """Envía los ganadores de una agencia específica"""
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
``` 

### Ejercicio 6

Los clientes ahora procesan múltiples apuestas simultáneamente mediante batches. En lugar de enviar una apuesta individual, cada cliente lee un archivo CSV con miles de apuestas y las procesa en grupos configurables.

El protocolo se extendió para soportar múltiples apuestas en un solo mensaje, separando cada apuesta con `;` dentro del payload. El servidor procesa todo el batch y responde con éxito solo si todas las apuestas fueron almacenadas correctamente.

Los archivos de datos se inyectan en los containers mediante volúmenes Docker, manteniendo la convención de que el cliente N utiliza el archivo `agency-{N}.csv`. 

El tamaño máximo de cada batch es configurable mediante `batch.maxAmount` en `config.yaml`, optimizado para no exceder 8kB por paquete y mejorar significativamente el throughput del sistema.

Los archivos CSV se procesan mediante streaming processing sin cargar completamente en memoria
- **Función `countBetsInFile()`:** Primera pasada para contar apuestas totales línea por línea
- **Función `processBetsInBatches()`:** Segunda pasada que procesa el archivo en pequeños batches
Solo mantiene en memoria el batch actual (ej: 100 apuestas máx). Los slices se limpian y reutilizan con `batch[:0]` después de cada envío.

#### Snippets importantes del código:

##### Lectura de CSV y Procesamiento por Batches:
```go
// client/common/client.go - Procesamiento de CSV línea por línea
func (c *Client) sendBetsStreaming(betsFile string, totalBets int) error {
    err := processBetsInBatches(betsFile, c.config.ID, totalBets, c.config.BatchMaxAmount, 
        func(batch []Bet, batchStart, batchEnd int, isLastBatch bool) error {
            select {
            case <-c.stop:
                return fmt.Errorf("client stopped")
            default:
                // Continuar con la ejecución
            }

            if err := sendBetBatch(c.conn, batch); err != nil {
                log.Errorf("action: send_bet_batch | result: fail | client_id: %v | error: %v",
                    c.config.ID, err)
                return err
            }

            ack, err := receiveAck(c.conn)
            last := batch[len(batch)-1]
            if err != nil {
                if err == io.EOF && isLastBatch {
                    log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
                    return nil
                }
                log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v",
                    c.config.ID, err)
                return err
            }

            log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s", last.Documento, last.Numero)
            return nil
        })

    if err != nil {
        return err
    }

    log.Infof("action: apuestas_enviadas | result: success | client_id: %v", c.config.ID)
    return nil
}
```

##### Procesar archivos sin cargarlos en memoria:
```go
// client/common/client.go - Procesamiento en streaming
func processBetsInBatches(filename string, agencyID string, totalBets int, batchSize int, processor func([]Bet, int, int, bool) error) error {
    csvFile, err := os.Open(filename)
    if err != nil {
        return fmt.Errorf("error opening file: %v", err)
    }
    defer csvFile.Close()

    scanner := bufio.NewScanner(csvFile)
    batch := make([]Bet, 0, batchSize)
    currentIndex := 0

    for scanner.Scan() {
        line := scanner.Text()
        if bet, err := parseBetLine(line, agencyID); err == nil {
            batch = append(batch, bet)

            // Cuando el batch está lleno o es la última apuesta, procesarlo
            if len(batch) >= batchSize || currentIndex+len(batch) >= totalBets {
                batchStart := currentIndex
                batchEnd := currentIndex + len(batch)
                isLastBatch := batchEnd >= totalBets

                if err := processor(batch, batchStart, batchEnd, isLastBatch); err != nil {
                    return err
                }

                currentIndex = batchEnd
                batch = batch[:0] // Limpiar el batch reutilizando memoria
            }
        } else {
            log.Warningf("Error parsing line '%s': %v", line, err)
        }
    }

    // Procesar último batch si quedó algo
    if len(batch) > 0 {
        batchStart := currentIndex
        batchEnd := currentIndex + len(batch)
        isLastBatch := true

        if err := processor(batch, batchStart, batchEnd, isLastBatch); err != nil {
            return err
        }
    }

    return nil
}
```

##### Protocolo:
```go
// client/common/protocol.go - Múltiples apuestas separadas por ;
func sendBetBatch(conn net.Conn, bets []Bet) error {
    var payloads []string
    for _, bet := range bets {
        betPayload := fmt.Sprintf("%s,%s,%s,%s,%s,%s",
            bet.Agencia,
            bet.Nombre,
            bet.Apellido,
            bet.Documento,
            bet.Nacimiento,
            bet.Numero,
        )
        payloads = append(payloads, betPayload)
    }

    payload := strings.Join(payloads, ";")
    data := []byte(payload)
    
    length := uint16(len(data))
    header := make([]byte, 2)
    binary.BigEndian.PutUint16(header, length)

    if err := writeAll(conn, header); err != nil {
        return fmt.Errorf("error sending header: %v", err)
    }

    if err := writeAll(conn, data); err != nil {
        return fmt.Errorf("error sending payload: %v", err)
    }

    return nil
}
```

##### Deserialización de apuestas:
```python
# server/common/protocol.py - Deserialización de múltiples apuestas
def parse_bet_batch_text(text: str) -> list[Bet]:
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
```

### Ejercicio 5

El cliente recibe los datos de una apuesta (nombre, apellido, DNI, nacimiento, número) a través de variables de entorno y los envía al servidor siguiendo un protocolo de comunicación personalizado. El servidor recibe la apuesta, la almacena usando la función `store_bets()` provista por la cátedra y responde con un ACK conteniendo el número apostado.

Se implementó un protocolo  usando sockets TCP. Utiliza un header de 2 bytes en formato big-endian que indica la longitud del payload, seguido del payload en formato CSV (`agencia,nombre,apellido,documento,nacimiento,numero`) y finalmente un ACK de 4 bytes en big-endian con el número apostado como confirmación.

Las funciones `readAll()` y `writeAll()` implementadas garantizan lectura y escritura completa de todos los bytes solicitados, evitando los problemas de short read/write. 

El módulo `protocol` encapsula toda la lógica de comunicación de red y la estructura `Bet` define el modelo de dominio para las apuestas.

#### Snippets del código:

##### Protocolo de Comunicación - Cliente (Go):
```go
// client/common/protocol.go
func sendBet(conn net.Conn, bet Bet) error {
    payload := fmt.Sprintf("%s,%s,%s,%s,%s,%s",
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

func receiveAck(conn net.Conn) (int, error) {
    buf := make([]byte, 4)
    if err := readAll(conn, buf); err != nil {
        return 0, fmt.Errorf("error reading ACK: %w", err)
    }

    ackNumber := int(binary.BigEndian.Uint32(buf))
    return ackNumber, nil
}
```

##### Funciones de lectura/escritura completa:
```go
// client/common/protocol.go - Evitar short read/write
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

func readAll(conn net.Conn, buf []byte) error {
    totalRead := 0
    for totalRead < len(buf) {
        n, err := conn.Read(buf[totalRead:])
        if err != nil {
            if err == io.EOF && totalRead > 0 {
                return fmt.Errorf("unexpected EOF, read %d bytes of %d", totalRead, len(buf))
            }
            return err
        }
        totalRead += n
    }
    return nil
}
```

##### Protocolo de Comunicación - Servidor (Python):
```python
# server/common/protocol.py
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

def send_ack(sock, bet: Bet):
    """Envía un ACK de 4 bytes big-endian con el número de la apuesta."""
    if bet is None:
        # Usar 0xFFFFFFFF (uint32) como código de error
        ack_value = 0xFFFFFFFF
    else:
        ack_value = bet.number
    ack = _pack_uint32_big_endian(ack_value)
    _send_all(sock, ack)
```

##### Manejo de Apuestas en el Servidor:
```python
# server/common/server.py
def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            bet = read_bet(client_sock)
            store_bets([bet])
            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')
            send_ack(client_sock, bet)
        except OSError as e:
            logging.error(f"action: apuesta_almacenada | result: fail | error: {e}")
        finally:
            client_sock.close()
```

### Ejercicio 4

Implementé el manejo de señales SIGTERM para realizar un graceful shutdown tanto en el servidor como en el cliente. En el servidor, registré un handler de señal que al recibir SIGTERM cierra el socket del servidor, termina el loop principal y registra los pasos del shutdown. En el cliente, configuré un canal de señales que al recibir SIGTERM invoca un método que cierra el canal de parada (stop), termina el loop de mensajes y cierra la conexión activa, asegurando que todos los file descriptors se cierren correctamente antes de que termine la aplicación principal.

En **server/main.py** `signal.signal(signal.SIGTERM, self.handle_sigterm)` capturar la señal, se modifica `self.running` a false, se dejan de aceptar conexiones y se cierra el socket.

#### Servidor
```python
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
```

#### Cliente
```go
type Client struct {
	config ClientConfig
	conn   net.Conn
	stop   chan struct{}
}
func main() {
   sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	go func() {
		<-sigChan
		client.StopClientLoop()
	}()

	client.StartClientLoop()
}

func (c *Client) StartClientLoop() {
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		// Create the connection the server in every loop iteration. Send an
		select {
		case <-c.stop:
			return
		default:
         // resto del código
      }
   }

func (c *Client) StopClientLoop() {
	close(c.stop)
	log.Infof("action: stop_client_loop | result: success | client_id: %v", c.config.ID)
	if c.conn != nil {
		c.conn.Close()
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}
}
```

### Ejercicio 3

Implementé validar-echo-server.sh que verifica automáticamente el correcto funcionamiento del servidor echo utilizando Docker y netcat. El script ejecuta un contenedor temporal con la imagen busybox conectado a la misma red Docker (tp0_testing_net) que el servidor, envía el mensaje "hola" usando netcat al puerto 12345, captura la respuesta del servidor y verifica que sea idéntica al mensaje enviado, cumpliendo así con el comportamiento esperado de un echo server. 

### Ejercicio 2

Implementé la inyección de archivos de configuración externos utilizando volúmenes Docker para evitar tener que reconstruir las imágenes cada vez que se modifica la configuración. Para lograr esto, eliminé la copia de archivos de configuración de los Dockerfiles (comentando COPY ./client/config.yaml /config.yaml en el cliente y agregando config.ini al .dockerignore del servidor), y luego configuré el montaje de volúmenes en Docker Compose para mapear los archivos de configuración del host directamente a los contenedores (./server/config.ini:/config.ini para el servidor y ./client/config.yaml:/config.yaml para el cliente), permitiendo así que cualquier cambio en estos archivos sea efectivo inmediatamente al reiniciar los contenedores sin necesidad de reconstruir las imágenes.

### Ejercicio 1

Implementé generar-compose.sh que automatiza la creación de archivos Docker Compose con una cantidad configurable de clientes. El script recibe dos parámetros: el nombre del archivo de salida (como docker-compose-dev.yaml) y la cantidad de clientes deseada, luego utiliza un bucle en bash para generar dinámicamente los servicios cliente con nombres secuenciales (client1, client2, client3, etc.), manteniendo la estructura de red, variables de entorno y dependencias necesarias para que cada cliente pueda comunicarse correctamente con el servidor.

#### Cómo ejecutar el ejercicio

1. **Generar el archivo Docker Compose:**
   ```bash
   ./generar-compose.sh NOMBRE-ARCHIVO CANT-CLIENTES
   ```

   Por ejemplo, si ejecutamos
   ```bash
   ./generar-compose.sh docker-compose-dev.yaml 5
   ```
   Se genera un archivo `docker-compose-dev.yaml` con 5 clientes (client1, client2, client3, client4, client5).

2. **Levantar el sistema:**
   ```bash
   make docker-compose-up
   ```

4. **Ver los logs:**
   ```bash
   make docker-compose-logs
   ```

5. **Detener el sistema:**
   ```bash
   make docker-compose-down
   ```

####
---

# TP0: Docker + Comunicaciones + Concurrencia

En el presente repositorio se provee un esqueleto básico de cliente/servidor, en donde todas las dependencias del mismo se encuentran encapsuladas en containers. Los alumnos deberán resolver una guía de ejercicios incrementales, teniendo en cuenta las condiciones de entrega descritas al final de este enunciado.

 El cliente (Golang) y el servidor (Python) fueron desarrollados en diferentes lenguajes simplemente para mostrar cómo dos lenguajes de programación pueden convivir en el mismo proyecto con la ayuda de containers, en este caso utilizando [Docker Compose](https://docs.docker.com/compose/).

## Instrucciones de uso
El repositorio cuenta con un **Makefile** que incluye distintos comandos en forma de targets. Los targets se ejecutan mediante la invocación de:  **make \<target\>**. Los target imprescindibles para iniciar y detener el sistema son **docker-compose-up** y **docker-compose-down**, siendo los restantes targets de utilidad para el proceso de depuración.

Los targets disponibles son:

| target  | accion  |
|---|---|
|  `docker-compose-up`  | Inicializa el ambiente de desarrollo. Construye las imágenes del cliente y el servidor, inicializa los recursos a utilizar (volúmenes, redes, etc) e inicia los propios containers. |
| `docker-compose-down`  | Ejecuta `docker-compose stop` para detener los containers asociados al compose y luego  `docker-compose down` para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene de versiones de desarrollo y recursos sin liberar. |
|  `docker-compose-logs` | Permite ver los logs actuales del proyecto. Acompañar con `grep` para lograr ver mensajes de una aplicación específica dentro del compose. |
| `docker-image`  | Construye las imágenes a ser utilizadas tanto en el servidor como en el cliente. Este target es utilizado por **docker-compose-up**, por lo cual se lo puede utilizar para probar nuevos cambios en las imágenes antes de arrancar el proyecto. |
| `build` | Compila la aplicación cliente para ejecución en el _host_ en lugar de en Docker. De este modo la compilación es mucho más veloz, pero requiere contar con todo el entorno de Golang y Python instalados en la máquina _host_. |

### Servidor

Se trata de un "echo server", en donde los mensajes recibidos por el cliente se responden inmediatamente y sin alterar. 

Se ejecutan en bucle las siguientes etapas:

1. Servidor acepta una nueva conexión.
2. Servidor recibe mensaje del cliente y procede a responder el mismo.
3. Servidor desconecta al cliente.
4. Servidor retorna al paso 1.


### Cliente
 se conecta reiteradas veces al servidor y envía mensajes de la siguiente forma:
 
1. Cliente se conecta al servidor.
2. Cliente genera mensaje incremental.
3. Cliente envía mensaje al servidor y espera mensaje de respuesta.
4. Servidor responde al mensaje.
5. Servidor desconecta al cliente.
6. Cliente verifica si aún debe enviar un mensaje y si es así, vuelve al paso 2.

### Ejemplo

Al ejecutar el comando `make docker-compose-up`  y luego  `make docker-compose-logs`, se observan los siguientes logs:

```
client1  | 2024-08-21 22:11:15 INFO     action: config | result: success | client_id: 1 | server_address: server:12345 | loop_amount: 5 | loop_period: 5s | log_level: DEBUG
client1  | 2024-08-21 22:11:15 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°1
server   | 2024-08-21 22:11:14 DEBUG    action: config | result: success | port: 12345 | listen_backlog: 5 | logging_level: DEBUG
server   | 2024-08-21 22:11:14 INFO     action: accept_connections | result: in_progress
server   | 2024-08-21 22:11:15 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:15 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°1
server   | 2024-08-21 22:11:15 INFO     action: accept_connections | result: in_progress
server   | 2024-08-21 22:11:20 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:20 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°2
server   | 2024-08-21 22:11:20 INFO     action: accept_connections | result: in_progress
client1  | 2024-08-21 22:11:20 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°2
server   | 2024-08-21 22:11:25 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:25 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°3
client1  | 2024-08-21 22:11:25 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°3
server   | 2024-08-21 22:11:25 INFO     action: accept_connections | result: in_progress
server   | 2024-08-21 22:11:30 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:30 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°4
server   | 2024-08-21 22:11:30 INFO     action: accept_connections | result: in_progress
client1  | 2024-08-21 22:11:30 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°4
server   | 2024-08-21 22:11:35 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2024-08-21 22:11:35 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°5
client1  | 2024-08-21 22:11:35 INFO     action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°5
server   | 2024-08-21 22:11:35 INFO     action: accept_connections | result: in_progress
client1  | 2024-08-21 22:11:40 INFO     action: loop_finished | result: success | client_id: 1
client1 exited with code 0
```


## Parte 1: Introducción a Docker
En esta primera parte del trabajo práctico se plantean una serie de ejercicios que sirven para introducir las herramientas básicas de Docker que se utilizarán a lo largo de la materia. El entendimiento de las mismas será crucial para el desarrollo de los próximos TPs.

### Ejercicio N°1:
Definir un script de bash `generar-compose.sh` que permita crear una definición de Docker Compose con una cantidad configurable de clientes.  El nombre de los containers deberá seguir el formato propuesto: client1, client2, client3, etc. 

El script deberá ubicarse en la raíz del proyecto y recibirá por parámetro el nombre del archivo de salida y la cantidad de clientes esperados:

`./generar-compose.sh docker-compose-dev.yaml 5`

Considerar que en el contenido del script pueden invocar un subscript de Go o Python:

```
#!/bin/bash
echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"
python3 mi-generador.py $1 $2
```

En el archivo de Docker Compose de salida se pueden definir volúmenes, variables de entorno y redes con libertad, pero recordar actualizar este script cuando se modifiquen tales definiciones en los sucesivos ejercicios.

### Ejercicio N°2:
Modificar el cliente y el servidor para lograr que realizar cambios en el archivo de configuración no requiera reconstruír las imágenes de Docker para que los mismos sean efectivos. La configuración a través del archivo correspondiente (`config.ini` y `config.yaml`, dependiendo de la aplicación) debe ser inyectada en el container y persistida por fuera de la imagen (hint: `docker volumes`).


### Ejercicio N°3:
Crear un script de bash `validar-echo-server.sh` que permita verificar el correcto funcionamiento del servidor utilizando el comando `netcat` para interactuar con el mismo. Dado que el servidor es un echo server, se debe enviar un mensaje al servidor y esperar recibir el mismo mensaje enviado.

En caso de que la validación sea exitosa imprimir: `action: test_echo_server | result: success`, de lo contrario imprimir:`action: test_echo_server | result: fail`.

El script deberá ubicarse en la raíz del proyecto. Netcat no debe ser instalado en la máquina _host_ y no se pueden exponer puertos del servidor para realizar la comunicación (hint: `docker network`). `


### Ejercicio N°4:
Modificar servidor y cliente para que ambos sistemas terminen de forma _graceful_ al recibir la signal SIGTERM. Terminar la aplicación de forma _graceful_ implica que todos los _file descriptors_ (entre los que se encuentran archivos, sockets, threads y procesos) deben cerrarse correctamente antes que el thread de la aplicación principal muera. Loguear mensajes en el cierre de cada recurso (hint: Verificar que hace el flag `-t` utilizado en el comando `docker compose down`).

## Parte 2: Repaso de Comunicaciones

Las secciones de repaso del trabajo práctico plantean un caso de uso denominado **Lotería Nacional**. Para la resolución de las mismas deberá utilizarse como base el código fuente provisto en la primera parte, con las modificaciones agregadas en el ejercicio 4.

### Ejercicio N°5:
Modificar la lógica de negocio tanto de los clientes como del servidor para nuestro nuevo caso de uso.

#### Cliente
Emulará a una _agencia de quiniela_ que participa del proyecto. Existen 5 agencias. Deberán recibir como variables de entorno los campos que representan la apuesta de una persona: nombre, apellido, DNI, nacimiento, numero apostado (en adelante 'número'). Ej.: `NOMBRE=Santiago Lionel`, `APELLIDO=Lorca`, `DOCUMENTO=30904465`, `NACIMIENTO=1999-03-17` y `NUMERO=7574` respectivamente.

Los campos deben enviarse al servidor para dejar registro de la apuesta. Al recibir la confirmación del servidor se debe imprimir por log: `action: apuesta_enviada | result: success | dni: ${DNI} | numero: ${NUMERO}`.



#### Servidor
Emulará a la _central de Lotería Nacional_. Deberá recibir los campos de la cada apuesta desde los clientes y almacenar la información mediante la función `store_bet(...)` para control futuro de ganadores. La función `store_bet(...)` es provista por la cátedra y no podrá ser modificada por el alumno.
Al persistir se debe imprimir por log: `action: apuesta_almacenada | result: success | dni: ${DNI} | numero: ${NUMERO}`.

#### Comunicación:
Se deberá implementar un módulo de comunicación entre el cliente y el servidor donde se maneje el envío y la recepción de los paquetes, el cual se espera que contemple:
* Definición de un protocolo para el envío de los mensajes.
* Serialización de los datos.
* Correcta separación de responsabilidades entre modelo de dominio y capa de comunicación.
* Correcto empleo de sockets, incluyendo manejo de errores y evitando los fenómenos conocidos como [_short read y short write_](https://cs61.seas.harvard.edu/site/2018/FileDescriptors/).


### Ejercicio N°6:
Modificar los clientes para que envíen varias apuestas a la vez (modalidad conocida como procesamiento por _chunks_ o _batchs_). 
Los _batchs_ permiten que el cliente registre varias apuestas en una misma consulta, acortando tiempos de transmisión y procesamiento.

La información de cada agencia será simulada por la ingesta de su archivo numerado correspondiente, provisto por la cátedra dentro de `.data/datasets.zip`.
Los archivos deberán ser inyectados en los containers correspondientes y persistido por fuera de la imagen (hint: `docker volumes`), manteniendo la convencion de que el cliente N utilizara el archivo de apuestas `.data/agency-{N}.csv` .

En el servidor, si todas las apuestas del *batch* fueron procesadas correctamente, imprimir por log: `action: apuesta_recibida | result: success | cantidad: ${CANTIDAD_DE_APUESTAS}`. En caso de detectar un error con alguna de las apuestas, debe responder con un código de error a elección e imprimir: `action: apuesta_recibida | result: fail | cantidad: ${CANTIDAD_DE_APUESTAS}`.

La cantidad máxima de apuestas dentro de cada _batch_ debe ser configurable desde config.yaml. Respetar la clave `batch: maxAmount`, pero modificar el valor por defecto de modo tal que los paquetes no excedan los 8kB. 

Por su parte, el servidor deberá responder con éxito solamente si todas las apuestas del _batch_ fueron procesadas correctamente.

### Ejercicio N°7:

Modificar los clientes para que notifiquen al servidor al finalizar con el envío de todas las apuestas y así proceder con el sorteo.
Inmediatamente después de la notificacion, los clientes consultarán la lista de ganadores del sorteo correspondientes a su agencia.
Una vez el cliente obtenga los resultados, deberá imprimir por log: `action: consulta_ganadores | result: success | cant_ganadores: ${CANT}`.

El servidor deberá esperar la notificación de las 5 agencias para considerar que se realizó el sorteo e imprimir por log: `action: sorteo | result: success`.
Luego de este evento, podrá verificar cada apuesta con las funciones `load_bets(...)` y `has_won(...)` y retornar los DNI de los ganadores de la agencia en cuestión. Antes del sorteo no se podrán responder consultas por la lista de ganadores con información parcial.

Las funciones `load_bets(...)` y `has_won(...)` son provistas por la cátedra y no podrán ser modificadas por el alumno.

No es correcto realizar un broadcast de todos los ganadores hacia todas las agencias, se espera que se informen los DNIs ganadores que correspondan a cada una de ellas.

## Parte 3: Repaso de Concurrencia
En este ejercicio es importante considerar los mecanismos de sincronización a utilizar para el correcto funcionamiento de la persistencia.

### Ejercicio N°8:

Modificar el servidor para que permita aceptar conexiones y procesar mensajes en paralelo. En caso de que el alumno implemente el servidor en Python utilizando _multithreading_,  deberán tenerse en cuenta las [limitaciones propias del lenguaje](https://wiki.python.org/moin/GlobalInterpreterLock).

## Condiciones de Entrega
Se espera que los alumnos realicen un _fork_ del presente repositorio para el desarrollo de los ejercicios y que aprovechen el esqueleto provisto tanto (o tan poco) como consideren necesario.

Cada ejercicio deberá resolverse en una rama independiente con nombres siguiendo el formato `ej${Nro de ejercicio}`. Se permite agregar commits en cualquier órden, así como crear una rama a partir de otra, pero al momento de la entrega deberán existir 8 ramas llamadas: ej1, ej2, ..., ej7, ej8.
 (hint: verificar listado de ramas y últimos commits con `git ls-remote`)

Se espera que se redacte una sección del README en donde se indique cómo ejecutar cada ejercicio y se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado (Parte 2) y los mecanismos de sincronización utilizados (Parte 3).

Se proveen [pruebas automáticas](https://github.com/7574-sistemas-distribuidos/tp0-tests) de caja negra. Se exige que la resolución de los ejercicios pase tales pruebas, o en su defecto que las discrepancias sean justificadas y discutidas con los docentes antes del día de la entrega. El incumplimiento de las pruebas es condición de desaprobación, pero su cumplimiento no es suficiente para la aprobación. Respetar las entradas de log planteadas en los ejercicios, pues son las que se chequean en cada uno de los tests.

La corrección personal tendrá en cuenta la calidad del código entregado y casos de error posibles, se manifiesten o no durante la ejecución del trabajo práctico. Se pide a los alumnos leer atentamente y **tener en cuenta** los criterios de corrección informados  [en el campus](https://campusgrado.fi.uba.ar/mod/page/view.php?id=73393).
