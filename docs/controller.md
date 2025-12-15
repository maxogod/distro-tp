# Estructura del Controller

La estructura del controller se organiza de la siguiente manera:

```sh
controller/
├── Dockerfile
├── cmd/
│   └── main.go
├── config/
│   └── config.go
├── config.yaml
└── internal/
    ├── handler/
    │   └── control_handler.go
    │   └── interface.go
    ├── server/
    │   └── server.go
    ├── sessions/
    │   ├── clients/
    │   │   └── client_session.go
    │   │   └── interface.go
    │   └── manager/
    │       └── client_manager.go
    │       └── interface.go
    └── storage/
        └── counter_storage.go
        └── interface.go
```

## Descripción de componentes

-   **`cmd/`**: Punto de entrada principal del sistema.
-   **`config/`**: Parseo y obtención de variables configurables.
-   **`internal/server/`**: Servidor principal que maneja clientes y mensajes de control.
-   **`internal/sessions/`**: Gestión de sesiones de clientes, incluyendo manejo de estado y coordinación de secuencias de control.
-   **`internal/storage/`**: Almacenamiento persistente de contadores de mensajes y estado de clientes para recuperación ante fallos.
-   **`Dockerfile`**: Imagen Docker para el despliegue del controller.
-   **`config.yaml`**: Archivo de configuración específico del controller.

## Funcionalidad

El controller coordina la ejecución de tareas para múltiples clientes, maneja el fin de transmisión mediante contadores de mensajes, y persiste el estado en disco para garantizar tolerancia a fallos. Utiliza un balanceo de carga determinístico basado en hash para asignar clientes a instancias específicas del controller.

## Ejecución

El controller inicia un servidor que acepta nuevos clientes a través de una cola de mensajes de control, restaura sesiones previas desde almacenamiento persistente, envía heartbeats para monitoreo de salud, y gestiona el ciclo de vida de las sesiones de clientes. Maneja señales de shutdown graceful para cerrar recursos adecuadamente.

## Config

En el archivo de `config.yaml` del controller, se definen las siguientes variables configurables:

```yaml
port: 8080
middleware:
    address: "amqp://guest:guest@rabbitmq:5672/"
heartbeat:
    host: "egg_of_life"
    port: 7777
    interval: 200
log:
    level: "development"
max:
    clients: 100
    unacked_counters: 5000
storage:
    path: "storage"
```

-   **`port`**: Puerto en el que el servidor HTTP del controller escucha.
-   **`middleware.address`**: Dirección del servidor RabbitMQ para mensajería.
-   **`heartbeat.host`** y **`heartbeat.port`**: Configuración para enviar heartbeats al Egg of Life.
-   **`heartbeat.interval`**: Intervalo en milisegundos entre envíos de heartbeats.
-   **`log.level`**: Nivel de logging.
-   **`max.clients`**: Número máximo de clientes simultáneos.
-   **`max.unacked_counters`**: Límite de contadores no confirmados.
-   **`storage.path`**: Ruta para almacenamiento persistente de estado.
