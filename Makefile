#GOPATH := $(CURDIR)/tmp/gopath

all: test install

build-erasure:
	cd erasure && make


test: build-erasure
	godep go test -race -coverprofile=cover/cover.out github.com/minio-io/minio
	godep go test -race github.com/minio-io/minio/cmd/minio

install: build-erasure
	godep go install github.com/minio-io/minio/cmd/minio

save:
	godep save ./...

restore:
	godep restore

env:
	godep go env

run: all
	minio gateway

cover: test
	go tool cover -html=cover/cover.out
