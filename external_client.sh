#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <task type> ..."
    exit 1
fi

echo "Building image"

docker build -t client -f ./src/gateway/Dockerfile . > /dev/null 2>&1

echo "Running"

docker run -v "$(pwd)/.data:/app/.data" -v "$(pwd)/.output1:/app/.output" --network tp1_tp_net --rm client:latest $@
