#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s
ARCH := $(shell uname -s)

all: getdeps install

checkdeps:
	@./checkdeps.sh

getdeps: checkdeps
	@go get github.com/tools/godep && echo "Installed godep"
	@go get golang.org/x/tools/cmd/cover && echo "Installed cover"

build-erasure: getdeps
	@godep go generate github.com/minio-io/minio/pkg/storage/erasure
	@godep go build github.com/minio-io/minio/pkg/storage/erasure

build-all: getdeps build-erasure
	@echo "Building Libraries"
	@godep go build ./...

test-all: build-all
	@echo "Running Test Suites:"
	@godep go test -race ./...

minio: build-all test-all

install: minio
	@godep go install github.com/minio-io/minio && echo "Installed minio"

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

clean:
	@echo "Cleaning up all the generated files"
	@rm -fv pkg/utils/split/TESTPREFIX.*
	@rm -fv cover.out
	@rm -fv pkg/storage/erasure/*.syso
