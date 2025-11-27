go test $(go list ./src/... | grep -v "test")  -cover -count=1
