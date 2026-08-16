# Regenerating the gRPC contract.
#
# The generated code is committed, so a consumer needs no protoc to build. It is regenerated
# only when the .proto changes, and the diff is reviewed like any other — a contract change
# that nobody reads is how an executor discovers at runtime that a field moved.

GOBIN := $(shell go env GOPATH)/bin

.PHONY: proto
proto:
	@command -v protoc >/dev/null || { echo "protoc not installed"; exit 1; }
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	PATH="$(PATH):$(GOBIN)" protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=github.com/abcdeqwer/go-job \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/abcdeqwer/go-job \
		--go-grpc_opt=require_unimplemented_servers=false \
		proto/gojob/v1/executor.proto

.PHONY: check
check:
	gofmt -l .
	go vet ./...
	go test ./...
