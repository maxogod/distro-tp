# Estructura del Worker Group By

La estructura de los workers se organiza de la siguiente manera:

```sh
group_by/
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
|   └── group_by_mock.go
├── config.yaml
└── Dockerfile
```

## Descripción de componentes

- **`business/`**: Lógica de negocio del agrupamiento.
- **`cmd/`**: Punto de entrada principal del sistema.
- **`config/`**: Parseo y obtención de variables configurables.
- **`internal/server/`**: Loop principal del servidor.
- **`internal/task_executor/`**: Despacho de acciones de agrupamiento según el tipo de tarea (`TaskType`).
- **`Dockerfile`**: Imagen Docker para el despliegue del worker.
- **`config.yaml`**: Archivo de configuración específico del worker.
- **`mock/`**: Datos mock para pruebas unitarias.

## Tareas del Worker

El worker ejecuta filtros sobre transacciones y sus ítems según el tipo de tarea:

- **Task 2**: Agrupa ítems de transacción por:

  - Par año-mes (`YYYY-MM`) y el ID del ítem (`item_id`).

- **Task 3**: Agrupa transacciones por:

  - Semestre de cada año (`H1` para la primera parte del año y `H2` para la segunda, quedando `year-semester`) y el ID de la tienda (`store_id`).

- **Task 4**: Agrupa transacciones por:

  - ID de usuario `user_id` e ID de la tienda `store_id`


## Ejecucion

En esencia, lo que hace el Group By (al igual que el resto de workers) es utilizar todos los componentes comunes definidos en `common/worker`, y definir su propia logica de negocio en el `TaskExecutor`.

Dado que el Group By solo va a comunicarse con los workers `Reducer`, iniciamos obteniendo la queue del `Reducer` (`reducer_queue`) y la queue donde le llegará la data al Group By desde el `Filter` (`filter_queue`).

Luego, inicializamos el `GroupService` que contiene toda la logica de negocio de agrupamiento necesaria para realizar las tasks.

Después, creamos el `TaskExecutor` propio del Group By (`GroupExecutor`), que implementa los metodos necesarios para cada task (2 a 4), y finalmente inicializamos el `MessageHandler` con la queue de input del Group By y el `TaskHandler` creado a partir del `TaskExecutor`.

Luego, simplemente llamamos al metodo `Start()` del `MessageHandler` para que comience a escuchar mensajes y procesarlos. Cada mensaje consumido de la input queue del Group By (`group_by_queue`) le llegará al MessageHandler por medio de un channel, para que luego sea procesado por el método correspondiente del `GroupExecutor`, que aplicará el agrupamiento (según lo indicado antes para cada task) y enviará el resultado a la queue correspondiente (`reducer_queue`) y se quedará con un grupo por cada criterio definido en cada tarea. Luego, todos los grupos generados son envíados en un único `GroupTransactionsBatch` al `Reducer`.

## Config

En el archivo de `config.yaml` del worker Group By solo se define la configuración necesaria para conectarse a RabbitMQ y el nivel de logger.Logger. No hay configuraciones específicas para este worker en particular.