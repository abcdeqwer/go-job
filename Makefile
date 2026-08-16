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

# The scheduler is concurrent by construction — several loops per tenant, several tenants per
# process, and a gRPC server on top — so the race detector is part of the check rather than
# something to reach for after a mystery. It needs GOJOB_TEST_DSN pointed at a MySQL the tests
# may create and drop schemas in; without it the database-backed tests skip.
.PHONY: check
check:
	gofmt -l .
	go vet ./...
	go test -race ./...
