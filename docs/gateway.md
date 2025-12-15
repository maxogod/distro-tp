# Estructura del Gateway

La estructura del gateway se organiza de la siguiente manera:

```sh
gateway/
├── Dockerfile
├── cmd/
│   └── main.go
├── config/
│   └── config.go
├── config.yaml
├── internal/
│   ├── handler/
│   │   ├── interface.go
│   │   ├── message_handler.go
│   │   └── message_handler_test.go
│   ├── healthcheck/
│   │   ├── interface.go
│   │   └── ping_server.go
│   ├── network/
│   │   ├── connection_manager.go
│   │   └── interface.go
│   ├── server/
│   │   └── server.go
│   └── sessions/
│       ├── clients/
│       │   ├── client_session.go
│       │   └── interface.go
│       └── manager/
│           ├── client_manager.go
│           └── interface.go
├── mock/
│   └── gateway_mock.go
```

## Descripción de componentes

-   **`cmd/`**: Punto de entrada principal del sistema.
-   **`config/`**: Parseo y obtención de variables configurables.
-   **`internal/server/`**: Servidor HTTP principal que maneja conexiones de clientes.
-   **`internal/sessions/`**: Gestión de sesiones de clientes, incluyendo manejo de estado y coordinación de secuencias de control.
-   **`internal/handler/`**: Manejo de mensajes entrantes y salientes con el middleware.
-   **`internal/healthcheck/`**: Servidor de healthcheck para monitoreo de salud.
-   **`internal/network/`**: Gestión de conexiones de red.
-   **`Dockerfile`**: Imagen Docker para el despliegue del gateway.
-   **`config.yaml`**: Archivo de configuración específico del gateway.
-   **`mock/`**: Datos mock para pruebas unitarias.

## Funcionalidad

El gateway actúa como el punto de entrada único al sistema distribuido y la interfaz exclusiva con los clientes. Recibe solicitudes de tareas de los clientes, coordina con el controller para inicializar sesiones, maneja el envío de datos de referencia y batches de transacciones, y finalmente devuelve los reportes procesados a los clientes. Implementa balanceo de carga aleatorio para distribuir la carga entre múltiples instancias de gateway.

## Ejecución

El gateway inicia un servidor TCP que acepta conexiones de clientes, establece sesiones, envía heartbeats para monitoreo de salud (al egg of life), y gestiona el flujo bidireccional de datos entre clientes y el sistema interno. Maneja señales de shutdown graceful para cerrar recursos adecuadamente.

Tambien posee un servidor HTTP para healthchecks, utilizado en docker compose para sincronizar que los clientes se conecten una vez algun gateway este vivo.

## Config

En el archivo de `config.yaml` del gateway, se definen las siguientes variables configurables:

```yaml
port: 8080
healthcheck:
    port: 8081
heartbeat:
    host: "egg_of_life"
    port: 7777
    interval: 200
middleware:
    address: "amqp://guest:guest@rabbitmq:5672/"
log:
    level: "development"
```

-   **`port`**: Puerto en el que el servidor HTTP del gateway escucha conexiones de clientes.
-   **`healthcheck.port`**: Puerto para el servidor de healthcheck.
-   **`heartbeat.host`** y **`heartbeat.port`**: Configuración para enviar heartbeats al Egg of Life.
-   **`heartbeat.interval`**: Intervalo en milisegundos entre envíos de heartbeats.
-   **`middleware.address`**: Dirección del servidor RabbitMQ para mensajería.
-   **`log.level`**: Nivel de logging.
