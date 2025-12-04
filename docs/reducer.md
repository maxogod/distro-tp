# Estructura del Worker Reducer

La estructura de los workers se organiza de la siguiente manera:

```sh
reducer/
├── business
│   ├── service.go
│   └── service_interface.go
├── cmd
│   └── main.go
├── config
│   └── config.go
├── internal
│   ├── server
│   │   └── server.go
│   └── task_executor
│       └── task_executor.go
├── mock
|   └── reducer_mock.go
├── config.yaml
└── Dockerfile
```

## Descripción de componentes

- **`business/`**: Lógica de negocio de la reducción.
- **`cmd/`**: Punto de entrada principal del sistema.
- **`config/`**: Parseo y obtención de variables configurables.
- **`internal/server/`**: Loop principal del servidor.
- **`internal/task_executor/`**: Despacho de acciones de reducción según el tipo de tarea (`TaskType`).
- **`Dockerfile`**: Imagen Docker para el despliegue del worker.
- **`config.yaml`**: Archivo de configuración específico del worker.
- **`mock/`**: Datos mock para pruebas unitarias.

## Tareas del Worker

El worker ejecuta una reducción sobre transacciones y sus ítems según el tipo de tarea:

- **Task 2**: Reduce un batch de ítems de transacción sumando:

  - Los subtotales de los items (obtiendo el `totalProfit`) y la cantidad de items (obteniendo el `totalSold`).

- **Task 3**: Reduce un batch de transacciones sumando:

  - El `finalAmount` de cada item obteniendo el `finalAmount` total del batch.

- **Task 4**: Reduce un batch de transacciones obteniendo:

  - El `TransactionQuantity` del batch definido como el total de transacciones dentro del grupo.


## Ejecucion

En esencia, lo que hace el Reducer (al igual que el resto de workers) es utilizar todos los componentes comunes definidos en `common/worker`, y definir su propia logica de negocio en el `TaskExecutor`.

Dado que el Reducer solo va a comunicarse con los workers `Aggregator`, iniciamos obteniendo la queue del `Aggregator` (`aggregator_queue`) y la queue donde le llegará la data al Reducer desde el `Group By` (`reducer_queue`).

Luego, inicializamos el `ReducerService` que contiene toda la logica de negocio de la reducción necesaria para realizar las tasks.

Después, creamos el `TaskExecutor` propio del Reducer (`ReducerExecutor`), que implementa los metodos necesarios para cada task (2 a 4), y finalmente inicializamos el `MessageHandler` con la queue de input del Reducer y el `TaskHandler` creado a partir del `TaskExecutor`.

Luego, simplemente llamamos al metodo `Start()` del `MessageHandler` para que comience a escuchar mensajes y procesarlos. Cada mensaje consumido de la input queue del Reducer (`reducer_queue`) le llegará al MessageHandler por medio de un channel, para que luego sea procesado por el método correspondiente del `ReducerExecutor`, que aplicará la reducción del batch (según lo indicado antes para cada task) y enviará el resultado a la queue correspondiente (`aggregator_queue`) y se quedará con un único batch. Luego, se envía ese batch reducido al `Agreggator`.

## Config

En el archivo de `config.yaml` del worker Reducer solo se define la configuración necesaria para conectarse a RabbitMQ y el nivel de logger.Logger. No hay configuraciones específicas para este worker en particular.