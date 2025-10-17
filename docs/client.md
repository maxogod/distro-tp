# Client executable - Gateway

Este programa es la interfaz que el usuario utiliza para comunicarse con el sistema distribuido de procesamiento.

La comunicacion es bilateral con el *Gateway controller* y no se comunica con ningun nodo/proceso detras del mismo, es decir que es el *'boundary'* entre usuario y sistema.

## How to run (standalone)

```bash
# Build the image
docker build -t client -f ./src/client/Dockerfile .

# Run the container and mount the data directory
docker run --rm -v $(pwd)/.data:/app/.data -it client:latest bash
```
