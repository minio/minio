GOPATH := $(CURDIR)/tmp/gopath

all: build copy_bin

copy_bin:
	cp tmp/gopath/bin/* bin/

stage_build:
	mkdir -p $(GOPATH)
	mkdir -p bin
	mkdir -p tmp/gopath/src/github.com/minios/minios
	rsync -a . tmp/gopath/src/github.com/minios/minios/
	rsync -a third_party/* tmp/gopath


build: stage_build
	go install github.com/minios/minios/minios
	go install github.com/minios/minios/miniosd
	cp tmp/gopath/bin/* bin/

clean:
	rm -rf tmp bin
