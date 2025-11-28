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
has_failure=0

for output_file in "${OUTPUT_FILES[@]}"; do
    filename=$(basename "$output_file")
    expected_file="$EXPECTED_FOLDER/$filename"

    if [ ! -f "$expected_file" ]; then
        echo "Expected file not found for $filename"
        has_failure=1
        continue
    fi

    python3 ./scripts/compare_output.py "$output_file" "$expected_file"
    if [ $? -ne 0 ]; then
        has_failure=1
    fi
done

if [ $has_failure -eq 1 ]; then
    exit 1
fi

exit 0

