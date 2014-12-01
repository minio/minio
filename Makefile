#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s

all: getdeps cover install

getdeps:
	@go get github.com/tools/godep && echo "Installing godep"
	@go get code.google.com/p/go.tools/cmd/cover && echo "Installing cover"

build-erasure:
	@cd pkgs/erasure && ${MAKE} ${MAKE_OPTIONS}

build-signify:
	@cd pkgs/signify && ${MAKE} ${MAKE_OPTIONS}

cover: build-erasure build-signify
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/storage
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/gateway

install: build-erasure
	@godep go install github.com/minio-io/minio/cmd/erasure-demo && echo "Installed erasure-demo into ${GOPATH}/bin"

save:
	godep save ./...

restore:
	godep restore

env:
	godep go env

run: all
	minio gateway
