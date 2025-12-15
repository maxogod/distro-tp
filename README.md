# Sistema Distribuido para Análisis de Ventas con Tolerancia a Fallos & Escalabilidad horizontal

## Language

📖 **Disponible en**: Español | [English](./README.en.md)

| Equipo          |
| --------------- |
| Federico Genaro |
| Santiago Sevitz |
| Maximo Utrera   |

Documentos de arquitectura:

-   [Entrega I: Diseño](./docs/diseño.pdf)
-   [Entrega II: Escalabilidad](./docs/escalabilidad.pdf)
-   [Entrega III: Multiclient](./docs/multiclient.pdf)
-   [Entrega IV: Tolerancia](./docs/tolerancia.pdf)

## Descripción General

Este proyecto presenta el diseño de un **sistema distribuido tolerante a fallos** para el análisis de datos de ventas de una cadena de cafeterías en Malasia. Fue desarrollado como Trabajo Práctico de la materia **Sistemas Distribuidos I (FIUBA)** y prioriza **escalabilidad, robustez y procesamiento distribuido por pipelines**.

El sistema permite procesar grandes volúmenes de transacciones enviadas por un cliente, aplicar transformaciones y agregaciones complejas, y devolver reportes consolidados, manteniendo consistencia incluso ante fallas parciales o catastroficas del sistema.

---

## Objetivos del Sistema

-   Procesar datasets de transacciones de gran tamaño de forma distribuida.
-   Soportar múltiples requisitos analíticos (filtrado, agregación, ranking, joins).
-   Garantizar tolerancia a fallos, manejo de duplicados e idempotencia.
-   Escalado horizontal e independientemente para cada tipo de nodo.

---

## Conjuntos de Datos

El sistema trabaja principalmente con:

-   **Transactions** (múltiples archivos)
-   **Transaction Items** (múltiples archivos)

Como datos de referencia (para joins):

-   Menu Items
-   Stores
-   Users

Otros datasets (Vouchers, Payment Methods) no se utilizan por no ser necesarios para los resultados esperados.

---

## Arquitectura General

<div align="center">
<img src="./docs/overview.png" alt="Arquitectura General" width="700"/>
</div>

### Componentes Principales

-   **Client**: envía datos y recibe reportes.
-   **Gateway**: punto de entrada al sistema y única interfaz con el cliente.
-   **Controller**: coordina la ejecución y controla el fin de transmisión.
-   **Workers**:

    -   Filter
    -   GroupBy
    -   Reducer
    -   Aggregator
    -   Joiner

-   **Middleware**: RabbitMQ como Message-Oriented Middleware.

La comunicación interna se realiza exclusivamente mediante **colas de mensajes**, permitiendo desacoplamiento y paralelismo.

---

## Procesamiento de Datos (Pipelines)

El sistema utiliza el modelo **Worker-per-filter**, donde cada etapa del pipeline está aislada:

1. **Filter**: aplica criterios temporales y de negocio.
2. **GroupBy**: agrupa registros por claves relevantes.
3. **Reducer**: ejecuta sumatorias o conteos.
4. **Aggregator**: consolida batches y aplica rankings (Top N).
5. **Joiner**: vincula datos procesados con datasets de referencia.

Cada requisito funcional define un pipeline específico combinando estas etapas.

---

## Flujo de Ejecución

1. El cliente solicita una tarea al Gateway.
2. El Controller inicializa la sequencia de control del cliente.
3. El cliente envía datasets de referencia.
4. El cliente envía batches de transacciones.
5. Los workers procesan los datos de forma encadenada.
6. El Aggregator consolida resultados.
7. El Joiner genera el reporte final.
8. El Gateway devuelve el reporte al cliente.

---

## Tolerancia a Fallos

### Mecanismos Implementados

-   **Health Check (Egg of Life)**: monitoreo por **heartbeats** UDP y reinicio automático de nodos.
-   **ACKs diferidos**: reencolado de mensajes ante fallos durante procesamiento.
-   **Persistencia en disco**:

    -   Joiner: datasets de referencia.
    -   Aggregator: batches procesados.
    -   Controller: estado de clientes.

-   **Manejo de duplicados**: números de secuencia para los paquetes por cliente.
-   **Mensajes idempotentes**: especialmente para el fin de transmisión.

### Componentes Críticos con Réplicas

-   Gateway (Se balancea la carga aleatoriamente)
-   Controller (Se balancea la carga definiendo deterministicamente el Controller por cliente utilizando una funcion de hash)
-   Aggregator

---

## Elección de Líder (EOL)

Se implementó el algoritmo **Bully** para formar una red de monitoreo entre los **Egg of life**, de manera de garantizar que el sistema no dependa de un solo nodo para reinicios automaticos, sino que estos se reinicien entre si en caso de falla. Con la premisa de un **mantainability** bajo esta solucion permitira nunca quedar sin este tipo de nodos, y de esta forma poder recuperarse de un caso de caida extrema del sistema (e.g. todos los workers, los gateways, los controllers y los egg of life excepto uno).

---

## Infraestructura y Despliegue

-   RabbitMQ con colas persistentes.
-   Workers desplegados como contenedores Docker.
-   Escalado horizontal mediante réplicas.
-   Orquestación con Docker Compose (en este caso, pero se puede desplegar en _Kubernetes_).

---

## Tecnologías Utilizadas

-   **Lenguajes**: Go, Python
-   **Mensajería**: RabbitMQ
-   **Serialización**: Protobuf
-   **Contenedores**: Docker
-   **Dataset**: Kaggle – Coffee Shop Transactions

---

## Documentación de Componentes

### Nodos

-   [Client](./docs/client.md)
-   [Gateway](./docs/gateway.md)
-   [Controller](./docs/controller.md)
-   [Filter](./docs/filter.md)
-   [Group By](./docs/group_by.md)
-   [Reducer](./docs/reducer.md)
-   [Aggregator](./docs/aggregator.md)
-   [Joiner](./docs/joiner.md)
-   [Egg of Life](./docs/egg_of_life.md)

### Mecanismos

-   [Worker Common](./docs/worker_common.md)
-   [Middleware](./docs/middleware.md)
-   [EOF](./docs/EOF.md)

---

## Referencias

-   [Kaggle: G Coffee Shop Transaction Dataset](https://www.kaggle.com/datasets/geraldooizx/g-coffee-shop-transaction-202307-to-202506)
-   [Notebook de validación FIUBA – Sistemas Distribuidos I](https://www.kaggle.com/code/gabrielrobles/fiuba-distribuidos-1-coffee-shop-analysis)
