#!/bin/bash

TEST_PATH="./src/tests/integration/"

for test_file in $(ls $TEST_PATH); do
  echo "Running tests in $test_file"
  go test "$TEST_PATH$test_file/..." -count=1
done
