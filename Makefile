#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s

all: getdeps cover install

checkdeps:
	@./checkdeps.sh

getdeps: checkdeps
	@go get github.com/tools/godep && echo "Installing godep"
	@go get code.google.com/p/go.tools/cmd/cover && echo "Installing cover"

build-erasure:
	@$(MAKE) $(MAKE_OPTIONS) -C pkgs/erasure/isal lib
	@godep go test -race github.com/minio-io/minio/pkgs/erasure
	@godep go test -coverprofile=cover.out github.com/minio-io/minio/pkgs/erasure

build-signify:
	@$(MAKE) $(MAKE_OPTIONS) -C pkgs/signify

build-crc32c:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/crc32c/cpu
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/crc32c

build-split: build-strbyteconv
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/split

build-strbyteconv:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/strbyteconv

cover: build-erasure build-signify build-split build-crc32c
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/storage
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/gateway

install: build-erasure
	@godep go install github.com/minio-io/minio/cmd/erasure-demo && echo "Installed erasure-demo into ${GOPATH}/bin"

save:
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

clean:
	@rm -v cover.out
