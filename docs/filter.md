# Estructura del Worker Filter

La estructura de los workers se organiza de la siguiente manera:

```sh
filter/
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
│       ├── task_config.go
│       └── task_executor.go
├── mock
|   └── filter_mock.go
├── config.yaml
└── Dockerfile
```

## Descripción de componentes

- **`business/`**: Lógica de negocio del filtro.
- **`cmd/`**: Punto de entrada principal del sistema.
- **`config/`**: Parseo y obtención de variables configurables.
- **`internal/server/`**: Loop principal del servidor.
- **`internal/task_executor/`**: Despacho de acciones de filtro según el tipo de tarea (`TaskType`).
- **`Dockerfile`**: Imagen Docker para el despliegue del worker.
- **`config.yaml`**: Archivo de configuración específico del worker.
- **`mock/`**: Datos mock para pruebas unitarias.

## Tareas del Worker

El worker ejecuta filtros sobre transacciones y sus ítems según el tipo de tarea:

- **Task 1**: Filtra transacciones por:

  - Rango de año `(2024 - 2025)`
  - Rango horario `(6 a 11 hs)`
  - Monto mínimo `(mayor a $75.0)`

- **Task 2**: Filtra ítems de transacción por:

  - Rango de año `(2024 - 2025)`

- **Task 3**: Filtra transacciones por:

  - Rango de año `(2024 - 2025)`
  - Rango horario `(6 a 11 hs)`

- **Task 4**: Filtra transacciones por:
  - Rango de año `(2024 - 2025)`


## Ejecucion

En esencia, lo que hace el Filter (al igual que el resto de workers) es utilizar todos los componentes comunes definidos en `common/worker`, y definir su propia logica de negocio en el `TaskExecutor`.

Dado que el Filter solo va a comunicarse con los workers `Group By` o `Aggregator` (dependiendo de la task a realizar), iniciamos obteniendo estas queues y la queue donde le llegará la data al Filter desde el `Gateway Controller`.

Luego, inicializamos el `FilterService` que contiene toda la logica de negocio de filtrado necesaria para realizar las tasks.

Después, creamos el `TaskExecutor` propio del Filter (`FilterExecutor`), que implementa los metodos necesarios para cada task (1 a 4), y finalmente inicializamos el `MessageHandler` con la queue de input del Filter y el `TaskHandler` creado a partir del `TaskExecutor`.

Luego, simplemente llamamos al metodo `Start()` del `MessageHandler` para que comience a escuchar mensajes y procesarlos. Cada mensaje consumido de la input queue del Filter (`filter_queue`) le llegará al MessageHandler por medio de un channel, para que luego sea procesado por el método correspondiente del `FilterExecutor`, que aplicará los filtros (según lo indicado antes para cada task) y enviará el resultado a la queue correspondiente (`group_by_queue` en las tasks 2, 3 y 4, y `aggregator_queue` en la task 1).

## Config

En el archivo de `config.yaml` del worker Filter, se definen las siguientes variables configurables utilizadas por el worker para realizar las tareas de filtrado de datos:

```yaml
filter:
  year:
    from: 2024
    to: 2025
  businessHours:
    from: 6
    to: 23
  totalAmountThreshold: 75.0
```

- **`filter.year.from`** y **`filter.year.to`**: Definen el rango de años para filtrar transacciones e ítems.
- **`filter.businessHours.from`** y **`filter.businessHours.to`**: Especifican el rango horario (en horas) para filtrar transacciones.
- **`filter.totalAmountThreshold`**: Establece el monto mínimo para filtrar transacciones.

Estos valores se obtienen en el código del worker a través del paquete de configuración y se utilizan en la lógica de filtrado implementada en el `FilterService`.