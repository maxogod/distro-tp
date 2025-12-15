# Estructura del Egg of Life

La estructura del egg of life se organiza de la siguiente manera:

```sh
egg_of_life/
├── Dockerfile
├── __init__.py
├── config.yaml
├── docker_runner.py
├── egg_of_life.py
├── leader_election/
│   ├── __init__.py
│   ├── leader_election.py
│   ├── tcp_handler.py
├── protocol/
│   └── __init__.py
└── udp_server.py
```

## Descripción de componentes

-   **`egg_of_life.py`**: Script principal que implementa el monitoreo de heartbeats y reinicio automático de nodos fallidos.
-   **`docker_runner.py`**: Maneja la ejecución, limpieza y reinicio de contenedores Docker.
-   **`udp_server.py`**: Servidor UDP para recibir heartbeats de otros nodos.
-   **`leader_election/`**: Implementación del algoritmo Bully para elección de líder entre instancias del Egg of Life.
-   **`config.yaml`**: Archivo de configuración específico del egg of life.
-   **`Dockerfile`**: Imagen Docker para el despliegue del egg of life.

## Funcionalidad

El Egg of Life (EOL) implementa el mecanismo de health check mediante heartbeats UDP para monitorear la salud de todos los nodos del sistema. Detecta timeouts y reinicia automáticamente contenedores fallidos usando Docker. Utiliza el algoritmo Bully para elección de líder, permitiendo que el sistema se recupere incluso de fallos catastróficos donde todos los nodos excepto uno caen.

## Ejecución

El script principal inicia hilos para enviar heartbeats a peers, recibir heartbeats, monitorear timeouts, y ejecutar comandos Docker para reiniciar nodos caídos. En caso de detectar un timeout, si es el líder, limpia y reinicia el contenedor fallido. También maneja la revocación de nodos previamente caídos cuando se recuperan.

## Config

En el archivo de `config.yaml` del egg of life, se definen las siguientes variables configurables:

```yaml
port: 7777
timeout_interval: 2
check_interval: 1
leader_election:
    port: 6666
    interval: 200
```

-   **`port`**: Puerto UDP para recibir heartbeats.
-   **`timeout_interval`**: Tiempo en segundos para considerar un nodo como caído.
-   **`check_interval`**: Intervalo en segundos para verificar timeouts.
-   **`leader_election.port`**: Puerto para comunicación en la elección de líder.
-   **`leader_election.interval`**: Intervalo en milisegundos para envío de heartbeats en la elección de líder.

## Hound of Doom

El script `scripts/hound_of_doom.py` es una herramienta de testing utilizada para simular fallos en el sistema. Mata contenedores Docker de manera aleatoria y configurable, permitiendo probar la efectividad del mecanismo de tolerancia a fallos implementado por el Egg of Life. Puede configurarse para apuntar a prefijos específicos de contenedores, excluir ciertos tipos, y ajustar intervalos de tiempo entre "ataques".
