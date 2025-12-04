#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <config_file>"
    exit 1
fi

CONFIG_FILE="$1"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file '$CONFIG_FILE' not found"
    exit 1
fi

parse_config() {
    local current_service=""
    local instances=0
    
    while IFS= read -r line; do
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        
        if [[ "$line" =~ ^[[:space:]]{2}([a-z_]+):[[:space:]]*$ ]]; then
            current_service="${BASH_REMATCH[1]}"
            instances=0
        fi
        
        if [[ "$line" =~ ^[[:space:]]{4}instances:[[:space:]]*([0-9]+) ]]; then
            instances="${BASH_REMATCH[1]}"
            
            if [ "$current_service" == "client" ]; then
                for ((i=1; i<=instances; i++)); do
                    folder=".output$i"
                    mkdir -p "$folder"
                    echo "Created: $folder"
                done
            elif [[ "$current_service" == "aggregator" || "$current_service" == "joiner" || "$current_service" == "controller" ]]; then
                for ((i=1; i<=instances; i++)); do
                    folder=".storage/${current_service}$i"
                    mkdir -p "$folder"
                    echo "Created: $folder"
                done
            fi
        fi
    done < "$CONFIG_FILE"
}

parse_config
echo "Created dirs."
