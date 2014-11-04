#GOPATH := $(CURDIR)/tmp/gopath

all: test install

test:
	godep go test github.com/minios/minios
	godep go test github.com/minios/minios/minio

install:
	godep go install github.com/minios/minios/minio

save:
	godep save ./...

restore:
	godep restore

env:
	godep go env
