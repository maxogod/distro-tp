# Estructura del Aggregator

La estructura de los workers se organiza de la siguiente manera:

```sh
aggregator/
├── business
│   ├── service.go
│   └── service_interface.go
├── cmd
│   └── main.go
├── cache
│   └── cache_interface.go
│   └── in_memory_service.go
├── config
│   └── config.go
├── internal
│   ├── server
│   │   └── server.go
│   └── task_executor
│       └── task_executor.go
│       └── finish_executor_interface.go
│       └── finish_executor.go
├── mock
|   └── aggregator_mock.go
├── config.yaml
└── Dockerfile
```

## Descripción de componentes

- **`business/`**: Lógica de negocio del aggregator.
- **`cmd/`**: Punto de entrada principal del sistema.
- **`config/`**: Parseo y obtención de variables configurables.
- **`internal/server/`**: Loop principal del servidor.
- **`internal/task_executor/`**: Despacho de acciones del aggregator según el tipo de tarea (`TaskType`).
- **`Dockerfile`**: Imagen Docker para el despliegue del worker.
- **`config.yaml`**: Archivo de configuración específico del worker.
- **`mock/`**: Datos mock para pruebas unitarias.

## Tareas del Worker

El worker almacena todos los batches que recibe de cada una de las tasks, diferenciando el almacenamiento por el ID del cliente en cuestión.

## Ejecucion

En esencia, lo que hace el Aggregator (al igual que el resto de workers) es utilizar todos los componentes comunes definidos en `common/worker`, y definir su propia logica de negocio en el `TaskExecutor`.

Dado que el Aggregator solo va a comunicarse con el `Gateway Controller`, iniciamos obteniendo exchange del `Gateway Controller` para mandar la data (`processed_data_exchange`) y la queue donde le llegará la data al Aggregator desde el `Joiner` (`aggregator_queue`).

Luego, inicializamos el `AggregatorService` que contiene toda la logica de negocio de necesaria para realizar la agregación de la data que nos llega de los workers..

Después, creamos el `TaskExecutor` propio del Joiner (`AggregatorExecutor`), que implementa los metodos necesarios para cada task (1 a 4), y que instancia un `FinishExecutor` que será utilizado para realizar acciones finales (envío de data procesada) una vez que se complete todo el procesamiento. Finalmente, inicializamos el `MessageHandler` con la queue de input del Aggregator y el `TaskHandler` creado a partir del `TaskExecutor`.

Luego, simplemente llamamos al metodo `Start()` del `MessageHandler` para que comience a escuchar mensajes y procesarlos. Cada mensaje consumido de la input queue del Aggregator (`aggregator_queue`) le llegará al MessageHandler por medio de un channel, para que luego sea procesado por el método correspondiente del `AggregatorExecutor`. Como dijimos antes, lo que se hará en cada task es almacenar la data recibida usando un `CacheService` para luego, al finalizar el procesamiento de todos los batches de data para un cliente en particular, enviar la data agregada al `Gateway Controller` mediante la `processed_data_exchange` usando el `clientId` como la routing key.

Al finalizar el procesamiento de todos los batches de data para un cliente en particular, el Aggregator recibirá un mensaje por el `FinishExchange`, el cual hará que se ejecute el `HandleFinishClient`, haciendo que comience a enviarse la data agregada al `Gateway Controller` como se indicó antes.

## Config

En el archivo de `config.yaml` del worker Aggregator solo se define la configuración necesaria para conectarse a RabbitMQ y el nivel de log. No hay configuraciones específicas para este worker en particular.