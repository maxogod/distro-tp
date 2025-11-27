#!/bin/bash

OUTPUT_FILES=(.output*/**)

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <expected_folder>"
    exit 1
elif [ ${#OUTPUT_FILES[@]} -eq 0 ]; then
    echo "No output files found."
    exit 1
fi

EXPECTED_FOLDER="$1"

for output_file in "${OUTPUT_FILES[@]}"; do
    filename=$(basename "$output_file")
    
    expected_file="$EXPECTED_FOLDER/$filename"
    
    if [ ! -f "$expected_file" ]; then
        echo "Expected file not found for $filename"
        continue
    fi

    python3 ./scripts/compare_output.py "$output_file" "$expected_file"
done

