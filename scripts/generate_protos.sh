#!/usr/bin/env bash
set -e

# Root paths
PROTO_DIR="src/common/protobufs"
OUT_DIR="src/common/models"

# Walk all .proto files
find "$PROTO_DIR" -name "*.proto" | while read -r proto; do
  echo "Compiling $proto -> $OUT_DIR"

  protoc -I="$PROTO_DIR" \
    --go_out="$OUT_DIR" --go_opt=paths=source_relative \
    "$proto"
done

echo "All .proto files compiled!"
