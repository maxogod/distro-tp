# Estructura del Worker Filter

La estructura de los workers sigue el template `worker_base` y se organiza de la siguiente manera:

```sh
filter/
├── business/
│   ├── service.go
│   └── service_test.go
├── cmd/
│   └── main.go
├── config/
│   └── config.go
├── config.yaml
├── Dockerfile
├── handler/
│   ├── handler_test.go
│   ├── task_config.go
│   └── task_handler.go
├── internal/
│   └── server/
│       └── server.go
└── test/
    └── mock/
        ├── input_mock.go
        └── output_mock.go
```

## Descripción de componentes

- **business/**: Lógica de negocio del filtro.
- **cmd/**: Punto de entrada principal del sistema.
- **config/**: Parseo y obtención de variables configurables.
- **internal/server/**: Loop principal del servidor.
- **handler/**: Despacho de acciones de filtro según el tipo de tarea (`TaskType`).
- **Dockerfile**: Imagen Docker para el despliegue del worker.
- **config.yaml**: Archivo de configuración específico del worker.
- **test/mock/**: Datos mock para pruebas unitarias.

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
  - Rango horario (6 a 11 hs)

- **Task 4**: Filtra transacciones por:
  - Rango de año `(2024 - 2025)`


## Ejecucion

TODO: agregar despues de implementar el server correctamente

## Config

TODO: agregar despues de implementar config correctamente