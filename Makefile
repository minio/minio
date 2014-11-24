#GOPATH := $(CURDIR)/tmp/gopath

all: test install

build-erasure:
	cd erasure && make


test: build-erasure
	godep go test -race -coverprofile=cover.out github.com/minio-io/minio

install: build-erasure
	godep go install github.com/minio-io/minio/cmd/minio-encode
	godep go install github.com/minio-io/minio/cmd/minio-decode

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
