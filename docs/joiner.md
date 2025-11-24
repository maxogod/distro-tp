# Estructura del Worker Joiner

La estructura de los workers se organiza de la siguiente manera:

```sh
joiner/
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
├── mock
|   └── joiner_mock.go
├── config.yaml
└── Dockerfile
```

## Descripción de componentes

- **`business/`**: Lógica de negocio del joiner.
- **`cmd/`**: Punto de entrada principal del sistema.
- **`config/`**: Parseo y obtención de variables configurables.
- **`internal/server/`**: Loop principal del servidor.
- **`internal/task_executor/`**: Despacho de acciones del joiner según el tipo de tarea (`TaskType`).
- **`Dockerfile`**: Imagen Docker para el despliegue del worker.
- **`config.yaml`**: Archivo de configuración específico del worker.
- **`mock/`**: Datos mock para pruebas unitarias.

## Tareas del Worker

El worker ejecuta un join sobre transacciones y sus ítems con reference datasets, según el tipo de tarea:

- **Task 2**: Reemplaza el `ItemId` por el `ItemName` en cada uno de los batches que recibe usando el dataset de `menu_items`.

- **Task 3**: Reemplaza el `StoreId` por el `StoreName` en cada uno de los batches que recibe usando el dataset de `stores`.

- **Task 4**: Reemplaza el `StoreId` por el `StoreName` y agrega el `Birthdate` del usuario a partir de su `UserId`, en cada uno de los batches que recibe, usando los datasets de `stores` y `users`.


## Ejecucion

En esencia, lo que hace el Joiner (al igual que el resto de workers) es utilizar todos los componentes comunes definidos en `common/worker`, y definir su propia logica de negocio en el `TaskExecutor`.

Dado que el Joiner solo va a comunicarse con el `Aggregator`, iniciamos obteniendo la queue del `Aggregator` (`aggregator_queue`) y la queue donde le llegará la data al Joiner desde el `Reducer` (`joiner_queue`).

Luego, inicializamos el `JoinerService` que contiene toda la logica de negocio de necesaria para realizar el join con los reference datasets en las tasks.

Después, creamos el `TaskExecutor` propio del Joiner (`JoinerExecutor`), que implementa los metodos necesarios para cada task (2 a 4), y finalmente inicializamos el `MessageHandler` con la queue de input del Joiner y el `TaskHandler` creado a partir del `TaskExecutor`.

Luego, simplemente llamamos al metodo `Start()` del `MessageHandler` para que comience a escuchar mensajes y procesarlos. Cada mensaje consumido de la input queue del Joiner (`joiner_queue`) le llegará al MessageHandler por medio de un channel, para que luego sea procesado por el método correspondiente del `JoinerExecutor`.

En el caso del Joiner, tenemos que hacer una distinción entre los batches que recibimos. Estos pueden ser de reference o de data, y tienen un tratamiento diferente en cada caso.

En el caso de los batches de reference dataset, lo que hace el joiner es almacenar esa data usando un `CacheService` para luego poder utilizarla en el proceso de join con los batches de data.

En el caso de los batches de data, se aplicará el join del batch con el reference dataset correspondiente (según lo indicado antes para cada task) y enviará el resultado a la queue correspondiente (`joiner_queue`). Luego, se envía cada batch joineado al `Aggregator`. En el caso de la task 4, el Joiner recibe un único `CountedUserTransactionBatch` con varios `CountedUserTransactions`. Por cada uno de estos, se aplica el join correspondiente (obteniendo nuevamente un `CountedUserTransactions`) y se colocan todos juntos en un nuevo `CountedUserTransactionBatch` con toda la data joineada, y este último es enviado al Aggregator.

Al finalizar el procesamiento de todos los batches de data para un cliente en particular, el Joiner recibirá un mensaje por el `FinishExchange`, el cual hará que se ejecute el `HandleFinishClient` haciendo que toda la reference data almacenada para ese cliente sea eliminada de la cache. El mensaje de finish es enviado por el `Gateway Controller` una vez que recibió toda la data procesada desde el `Aggregator`.

## Config

En el archivo de `config.yaml` del worker Joiner solo se define la configuración necesaria para conectarse a RabbitMQ y el nivel de logger.Logger. No hay configuraciones específicas para este worker en particular.