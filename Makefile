#GOPATH := $(CURDIR)/tmp/gopath

all: test install

build-erasure:
	cd pkgs/erasure && make

build-signify:
	cd pkgs/signify && make

test: build-erasure build-signify
	godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/storage
	godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkgs/gateway

install: build-erasure
	godep go install github.com/minio-io/minio/cmd/minio-demo

save:
	godep save ./...

restore:
	godep restore

env:
	godep go env

run: all
	minio gateway

cover: test
	go tool cover -html=cover.out
