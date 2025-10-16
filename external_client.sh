#!/bin/bash

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <task type>"
    exit 1
fi

docker run -v "$(pwd)/.data:/app/.data" -v "$(pwd)/.output1:/app/.output" --network tp1_tp_net --rm client:latest $1 .
