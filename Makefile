#GOPATH := $(CURDIR)/tmp/gopath

all: test install

test:
	mkdir -p cover
	godep go test -race -coverprofile=cover/cover.out github.com/minios/minios
	godep go test -race github.com/minios/minios/minio

install:
	godep go install -race github.com/minios/minios/minio

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
