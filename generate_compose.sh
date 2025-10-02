#!/bin/bash

CONFIG_FILE="${1:-config.yaml}"
OUTPUT_FILE="${2:-docker-compose.yaml}"

# header
echo "name: tp1
services:" > "$OUTPUT_FILE"

echo "  rabbitmq:
    container_name: rabbitmq
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    image: rabbitmq:latest
    ports:
      - '5670:5672'
      - '15670:15672'
    networks:
      - tp_net
" >> "$OUTPUT_FILE"

while IFS= read -r line; do
    [[ "$line" =~ ^services:?$ ]] && continue

    IFS=":" read -r service replicas <<< "$line"
    service=$(echo "$service" | xargs)
    replicas=$(echo "$replicas" | xargs)

    for i in $(seq 1 "$replicas"); do
        if [ "$replicas" -eq 1 ]; then
            name="$service"
        else
            name="${service}${i}"
        fi
        echo "  ${name}:
    container_name: ${name}
    build:
      dockerfile: ./src/${service}/Dockerfile
    image: ${service}:latest
    networks:
      - tp_net
    depends_on:
      - rabbitmq" >> "$OUTPUT_FILE"

    if [ "$service" = "gateway_controller" ]; then
            echo "    ports:
      - '8080:8080'" >> "$OUTPUT_FILE"
    fi

    if [ "$service" = "gateway" ]; then
        echo '    entrypoint: ["/app/app", "t1"]
    volumes:
      - ./.data:/app/.data
      - ./.output:/app/.output
' >> "$OUTPUT_FILE"
    else
        echo "    volumes:
      - ./src/${service}/config.yaml:/config.yaml
" >> "$OUTPUT_FILE"
        fi
    done
done < "$CONFIG_FILE"

# networks
echo "networks:
  tp_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> "$OUTPUT_FILE"