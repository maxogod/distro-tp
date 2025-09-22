# Estructura Worker Base
La estructura de todos los workers se define mediante el template worker_base que tiene la siguiente organización:

```sh
worker_base/
├── business/
│   └── service.go
├── cmd/
│   └── main.go
├── config/
│   └── config.go
├── config.yaml
├── Dockerfile
└── internal/
    └── server/
        └── server.go
```
## Descripción de componentes

**business/:** Contiene toda la lógica de negocio del worker
**cmd/:** Punto de entrada principal del sistema
**config/:** Gestión del parseo y obtención de variables configurables
**internal/server/:** Implementación del loop principal del servidor
**Dockerfile:** Imagen de Docker para el despliegue del worker
**config.yaml:** Archivo de configuración específico del worker

## Creación de workers
Para crear un nuevo worker, utiliza el script automatizado:
```sh
./create_worker.sh <WORKER_NAME>
```
Nota importante: Ejecuta este script desde el directorio raíz del proyecto. El script generará automáticamente:

- Toda la estructura de directorios necesaria
- Las dependencias preconfiguradas
- El Dockerfile personalizado para el worker

Esta automatización garantiza consistencia y reduce el tiempo de setup inicial para nuevos workers.