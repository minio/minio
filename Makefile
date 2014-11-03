GOPATH := $(CURDIR)/tmp/gopath

all: build test copy_bin

copy_bin:
	cp tmp/gopath/bin/* bin/

stage_build:
	mkdir -p $(GOPATH)
	mkdir -p bin
	mkdir -p tmp/gopath/src/github.com/minios/minios
	rsync -a . tmp/gopath/src/github.com/minios/minios/
	rsync -a third_party/* tmp/gopath

test:
	go test github.com/minios/minios
	go test github.com/minios/minios/minio

build: stage_build
	go install github.com/minios/minios/minio

clean:
	rm -rf tmp bin
