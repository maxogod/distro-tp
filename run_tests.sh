#!/bin/bash

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Running tests for all workers in src directory...${NC}"

# Helper to extract test status and name
parse_test_line() {
    local line="$1"
    local status="$2"
    # Remove status and any trailing "(...)" 
    echo "$line" | sed "s/^--- $status:[[:space:]]*//" | sed 's/ (.*//'
}

# Run tests in a given directory
run_tests_in_dir() {
    local test_dir="$1"
    local worker_name="$2"

    echo -e "${BLUE}Testing $worker_name:${NC}"

    # Run tests and capture output
    local output
    output=$(go test -v "./$test_dir" 2>&1 || true)

    # Track current test name
    local current_test=""

    # Process output line by line
    while IFS= read -r line; do
        case "$line" in
            "=== RUN"* )
                current_test="${line#=== RUN }"
                ;;
            "--- PASS"* )
                test_name=$(parse_test_line "$line" "PASS")
                echo -e "  $worker_name.$test_name | ${GREEN}PASS${NC}"
                ;;
            "--- FAIL"* )
                test_name=$(parse_test_line "$line" "FAIL")
                echo -e "  $worker_name.$test_name | ${RED}FAIL${NC}"
                # Optional: show lines with "expected" keyword
                error_msg=$(echo "$output" | grep expected || true)
                if [ -n "$error_msg" ]; then
                    echo -e "    ${RED}$error_msg${NC}"
                fi
                ;;
            "--- SKIP"* )
                test_name=$(parse_test_line "$line" "SKIP")
                echo -e "  $worker_name.$test_name | ${YELLOW}SKIP${NC}"
                ;;
        esac
    done <<< "$output"

    echo ""
}

# Main loop: look for test directories
for worker_dir in src/*/; do
    worker_name=$(basename "$worker_dir")
    test_dir="$worker_dir/test"

    if [ -d "$test_dir" ]; then
        run_tests_in_dir "$test_dir" "$worker_name"
    fi
done

echo -e "${BLUE}Test run completed${NC}"