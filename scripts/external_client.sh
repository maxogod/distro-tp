#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <task type> ..."
    exit 1
fi

echo "Building image"

docker build -t client -f ./src/client/Dockerfile . > /dev/null 2>&1

echo "Running"

docker run -v "$(pwd)/.data:/app/.data" -v "$(pwd)/.output1:/app/.output" -v $(pwd)/src/client/config.yaml:/app/config.yaml --network distro_tp_net --rm client:latest $@
