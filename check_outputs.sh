#!/bin/bash

# Find all files matching .output*/task1_output.csv
files=(.output*/task1_output.csv)
data=(.data/transactions/*)

# Check if any files were found
if [ ${#files[@]} -eq 0 ]; then
    echo "No matching files found."
    exit 1
fi

# Get row count for the first file
first_count=$(wc -l < "${files[0]}")

# Flag to track if all counts match
all_match=true

# Check each file
for file in "${files[@]}"; do
    count=$(wc -l < "$file")
    echo "$file: $count rows"
    if [ "$count" -ne "$first_count" ]; then
        all_match=false
    fi
done

if $all_match; then
    echo "All files have the same number of rows: $first_count"
else
    echo "Row counts differ between files."
fi

total_count=0
for datafile in "${data[@]}"; do
    count=$(wc -l < "$datafile")
    total_count=$((total_count + count))
done
total_count=$((total_count - $(ls -l ${data[@]} | wc -l) + 1))
echo "Total rows in .data/transactions/*: $total_count"
