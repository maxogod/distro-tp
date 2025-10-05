# Worker Common
Esta carpeta contiene componentes reutilizables entre los distintos workers. Esto se categoriza en 3 partes:

- Data Handler
- Task Handler
- Message Handler

## Data Handler
La interfaz `DataHandler` define el contrato que deben implementar los workers para procesar datos recibidos en el sistema distribuido. Su propósito principal es abstraer la lógica de manejo de datos, finalización de clientes y cierre de recursos asociados al worker.

- `HandleData(dataEnvelope *protocol.DataEnvelope) error`: Método encargado de procesar un paquete de datos (`DataEnvelope`). Permite que el worker reciba y maneje la información enviada por los clientes o por otros componentes del sistema.
- `HandleFinishClient(clientID string) error`: Método llamado cuando un cliente ha finalizado su comunicación. Permite al worker realizar tareas de limpieza o actualización relacionadas con el cliente identificado por `clientID`.
- `Close() error`: Método para liberar recursos y cerrar el handler de forma segura, asegurando que no queden procesos o conexiones abiertas.

Esta interfaz debe ser utilizada junto con la estructura `MessageHandler` para garantizar una integración adecuada en el flujo de trabajo de los workers.


## Message Handler
El `MessageHandler` es un componente central diseñado para gestionar el flujo de mensajes en un sistema distribuido. Se encarga de consumir mensajes de las colas de entrada, procesarlos mediante un DataHandler, y enviarlos a las colas de salida. Además, administra el estado específico de cada cliente y ofrece funcionalidad para finalizar sesiones de clientes de manera controlada.
