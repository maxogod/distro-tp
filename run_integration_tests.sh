#!/bin/bash

TEST_PATH="./src/tests/integration/"

echo "Remember these tests need a rabbit instance up!"
echo "Run: docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management"

for test_file in $(ls $TEST_PATH); do
  echo "Running tests in $test_file"
  go test "$TEST_PATH$test_file/..." -count=1
done
