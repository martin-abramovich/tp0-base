### Ejercicio 8

Implementé que el servidor ahora maneje conexiones y procese mensajes en paralelo utilizando multithreading. Implementé un `ThreadPoolExecutor` con hasta 20 workers que permite atender múltiples clientes simultáneamente. La arquitectura funciona con un thread principal que acepta nuevas conexiones mientras que los threads workers del pool se encargan de procesar los mensajes de cada cliente de forma independiente y paralela.

Para garantizar la consistencia de datos usé mecanismos de sincronización. El estado del sorteo está protegido por `_state_lock` (un RLock que maneja `_finished_agencies` y `_lottery_done`), mientras que las conexiones pendientes están protegidas por `_pending_lock` para el diccionario `_pending_winners_requests`. Además, implementé `ThreadSafeStorage` como wrapper con RLock para todas las operaciones de almacenamiento, asegurando que múltiples threads puedan acceder al storage sin corromper los datos. El graceful shutdown también fue mejorado para coordinar el cierre del ThreadPoolExecutor y notificar a todas las conexiones pendientes antes del shutdown.

Aunque Python tiene el Global Interpreter Lock (GIL) que previene la ejecución simultánea de código Python, nuestro servidor es principalmente I/O-bound, realizando operaciones como `socket.accept()`, `socket.recv()`, `socket.send()`. Durante estas operaciones de I/O bloqueantes, el GIL se libera automáticamente, permitiendo que otros threads ejecuten código Python. Como nuestro servidor pasa la mayor parte del tiempo esperando I/O de red en lugar de realizar cálculos intensivos de CPU, el multithreading es efectivo.

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

Después de enviar todas las apuestas, cada cliente notifica al servidor que terminó. El servidor espera que las 5 agencias reporten finalización antes de realizar el sorteo.

Una vez completado el sorteo, los clientes pueden consultar la lista de ganadores específicos de su agencia. El sistema maneja consultas tempranas manteniendo conexiones activas hasta que el sorteo esté disponible.

Agregué dos nuevos comandos: `END|{agency_id}` para notificar finalización y `GET_WINNERS|{agency_id}` para consultar ganadores. 

El servidor registra las agencias finalizadas en `_finished_agencies`, y solo ejecuta el sorteo cuando `_finished_agencies` es igual a EXPECTED_AGENCIES que se pasa como variable de entorno. Caso contrario, el default configurado en server/config.ini es 5.

El servidor utiliza las funciones `load_bets()` y `has_won()` para determinar ganadores y responde con `WINNERS|{lista_dni}` conteniendo únicamente los DNI ganadores de cada agencia. Si el sorteo aún no fue realizado, se envía `NOT_READY`, se agrega a `pending_winners_requests` que es un diccionario que guarda las conexiones de clientes que están esperando los resultados del sorteo. La clave (int) es el ID de la agencia y el valor (object) es el socket de conexión del cliente. Esto me evitó que los clientes tengas que hacer polling hasta obtener los resultados.

Además. se reutiliza la misma conexión TCP para envío de apuestas, notificación de fin y consulta de ganadores. 

### Ejercicio 6

Los clientes ahora procesan múltiples apuestas simultáneamente mediante batches. En lugar de enviar una apuesta individual, cada cliente lee un archivo CSV con miles de apuestas y las procesa en grupos configurables.

El protocolo se extendió para soportar múltiples apuestas en un solo mensaje, separando cada apuesta con `;` dentro del payload. El servidor procesa todo el batch y responde con éxito solo si todas las apuestas fueron almacenadas correctamente.

Los archivos de datos se inyectan en los containers mediante volúmenes Docker, manteniendo la convención de que el cliente N utiliza el archivo `agency-{N}.csv`. 

El tamaño máximo de cada batch es configurable mediante `batch.maxAmount` en `config.yaml`, optimizado para no exceder 8kB por paquete y mejorar significativamente el throughput del sistema.

Los archivos CSV se procesan mediante **streaming processing** sin cargar completamente en memoria
- **Función `countBetsInFile()`:** Primera pasada para contar apuestas totales línea por línea
- **Función `processBetsInBatches()`:** Segunda pasada que procesa el archivo en pequeños batches
Solo mantiene en memoria el batch actual (ej: 100 apuestas máx). Los slices se limpian y reutilizan con `batch[:0]` después de cada envío.

**Protocolo de Comunicación por Batches:**
- Header: 2 bytes en big-endian indicando la longitud total del payload
- Payload: Múltiples apuestas en formato CSV separadas por `;` 
  - Formato por apuesta: `agencia,nombre,apellido,documento,nacimiento,numero`
  - Ejemplo: `1,Juan,Pérez,12345678,1990-01-01,1234;1,Ana,García,87654321,1985-06-15,5678`
- ACK: 4 bytes en big-endian con el número de la última apuesta procesada o 0xFFFFFFFF en caso de error
- Una sola conexión TCP por cliente reutilizada para todos los batches

#### Cómo ejecutar el ejercicio

1. **Extraer los datos de las agencias:**

2. **Configurar el tamaño de batch:**
   Modificar en `client/config.yaml`:
   ```yaml
   batch:
     maxAmount: 100  # Número máximo de apuestas por batch
   ```

3. **Generar el archivo Docker Compose con múltiples clientes:**
   ```bash
   ./generar-compose.sh docker-compose-dev.yaml 4
   ```

4. **Levantar el sistema:**
   ```bash
   make docker-compose-up
   ```

5. **Ver los logs para verificar el procesamiento por batches:**
   ```bash
   make docker-compose-logs
   ```
   
   Ejemplo
   ```
   server | action: apuesta_recibida | result: success | cantidad: 100
   client1 | action: apuesta_enviada | result: success | dni: 30904465 | numero: 2201
   ```

6. **Detener el sistema:**
   ```bash
   make docker-compose-down
   ```


### Ejercicio 5

El cliente recibe los datos de una apuesta (nombre, apellido, DNI, nacimiento, número) a través de variables de entorno y los envía al servidor siguiendo un protocolo de comunicación personalizado. El servidor recibe la apuesta, la almacena usando la función `store_bets()` provista por la cátedra y responde con un ACK conteniendo el número apostado.

Se implementó un protocolo  usando sockets TCP. Utiliza un header de 2 bytes en formato big-endian que indica la longitud del payload, seguido del payload en formato CSV (`agencia,nombre,apellido,documento,nacimiento,numero`) y finalmente un ACK de 4 bytes en big-endian con el número apostado como confirmación.

Las funciones `readAll()` y `writeAll()` implementadas garantizan lectura y escritura completa de todos los bytes solicitados, evitando los problemas de short read/write. 

El módulo `protocol` encapsula toda la lógica de comunicación de red y la estructura `Bet` define el modelo de dominio para las apuestas.

#### Cómo ejecutar el ejercicio

1. **Configurar las variables de entorno para las apuestas:**
   Las apuestas se configuran mediante variables de entorno en el `docker-compose-dev.yaml`. Por ejemplo:
   ```yaml
   environment:
     - CLI_ID=1
     - NOMBRE=Santiago Lionel  
     - APELLIDO=Lorca
     - DOCUMENTO=30904465
     - NACIMIENTO=1999-03-17
     - NUMERO=7574
   ```

### Ejercicio 4

Implementé el manejo de señales SIGTERM para realizar un graceful shutdown tanto en el servidor como en el cliente. En el servidor, registré un handler de señal que al recibir SIGTERM cierra el socket del servidor, termina el loop principal y registra los pasos del shutdown. En el cliente, configuré un canal de señales que al recibir SIGTERM invoca un método que cierra el canal de parada (stop), termina el loop de mensajes y cierra la conexión activa, asegurando que todos los file descriptors se cierren correctamente antes de que termine la aplicación principal.

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
