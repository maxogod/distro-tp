#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Running tests for all workers in src directory...${NC}"

for dir in src/*/; do
    DIR_NAME=$(basename "$dir")
    echo -e "${YELLOW}Testing $DIR_NAME...${NC}"

    test_output=$(go test "./src/$DIR_NAME/..." 2>&1)
    cover_output=$(go test "./src/$DIR_NAME/..." -cover 2>&1)

    # Filter lines
    while IFS= read -r line; do
        if [[ "$line" == ok* ]]; then
            echo -e "${GREEN}$line${NC}"
        elif [[ "$line" == FAIL* ]]; then
            echo -e "${RED}$line${NC}"
        elif [[ "$line" == "--- FAIL"* ]]; then
            echo -e "${RED}$line${NC}"
        elif [[ "$line" == *"Expected"* || "$line" == *"got"* ]]; then
            echo -e "${RED}$line${NC}"
        elif [[ "$line" == coverage* ]]; then
            echo "$line"
        fi
    done <<< "$test_output"

    while IFS= read -r line; do
        if [[ "$line" == ok* ]]; then
            echo -e "${GREEN}$line${NC}"
        elif [[ "$line" == FAIL* ]]; then
            echo -e "${RED}$line${NC}"
        elif [[ "$line" == coverage* ]]; then
            echo "$line"
        fi
    done <<< "$cover_output"

    echo ""
done

echo -e "${BLUE}Test run completed${NC}"