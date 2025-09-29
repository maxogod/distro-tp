#!/bin/bash

# Check if worker name parameter is provided
if [ $# -eq 0 ]; then
    echo "Error: Worker name is required"
    echo "Usage: $0 <worker_name>"
    echo "Worker name can only contain letters (a-z, A-Z)"
    exit 1
fi

WORKER_NAME=$1

echo "Creating worker: $WORKER_NAME"

WORKER_NAME_LOWER=$(echo "$WORKER_NAME" | tr '[:upper:]' '[:lower:]')

cp -r src/worker_base "src/$WORKER_NAME_LOWER"

echo "Updating Dockerfile..."
sed -i "s#src/worker_base/#src/$WORKER_NAME_LOWER/#g" "src/$WORKER_NAME_LOWER/Dockerfile"
sed -i "s#./src/worker_base/cmd#./src/$WORKER_NAME_LOWER/cmd#g" "src/$WORKER_NAME_LOWER/Dockerfile"
sed -i "s#bin/worker#bin/$WORKER_NAME_LOWER#g" "src/$WORKER_NAME_LOWER/Dockerfile"
sed -i "s#/worker#/$WORKER_NAME_LOWER#g" "src/$WORKER_NAME_LOWER/Dockerfile"

echo "Updating Go imports..."
find "src/$WORKER_NAME_LOWER" -name "*.go" -type f -exec sed -i "s#github.com/maxogod/distro-tp/src/worker_base#github.com/maxogod/distro-tp/src/$WORKER_NAME_LOWER#g" {} \;

if [ -f "src/$WORKER_NAME_LOWER/go.mod" ]; then
    sed -i "s#worker_base#$WORKER_NAME_LOWER#g" "src/$WORKER_NAME_LOWER/go.mod"
fi

echo "Worker '$WORKER_NAME' created successfully in src/$WORKER_NAME_LOWER!"
echo "Updated imports from 'github.com/maxogod/distro-tp/src/worker_base' to 'github.com/maxogod/distro-tp/src/$WORKER_NAME_LOWER'"