build-options:
	buf generate --template proto/options/buf.gen.yaml --path proto/options
build-example:
	go install
	go install github.com/favadi/protoc-go-inject-tag@latest
	go install github.com/mitchellh/protoc-gen-go-json@latest
	cd example; buf generate;
	protoc-go-inject-tag -input "example/*.*.*.go"
	protoc-go-inject-tag -input "example/*.*.go"
clean:
	rm -rf example/google
	rm -rf example/options
	rm -rf example/*.go
generate: clean build-options build-example
test: generate
	go test -v ./test