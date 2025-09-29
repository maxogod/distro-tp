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
      - '5672:5672'
      - '15672:15672'
    networks:
      - tp_net
" >> "$OUTPUT_FILE"

while IFS= read -r line; do
    [[ "$line" =~ ^services ]] && continue

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
      context: ./src/${service}
      dockerfile: Dockerfile
    image: ${service}:latest
    networks:
      - tp_net
    volumes:
      - ./${service}/config.yaml:/config.yaml
" >> "$OUTPUT_FILE"
    done
done < "$CONFIG_FILE"

# networks
echo "networks:
  tp_net:
    driver: default" >> "$OUTPUT_FILE"