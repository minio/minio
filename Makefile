LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)
DOCKER_BIN := $(shell which docker)
PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
DOCKER_LDFLAGS := '$(LDFLAGS) -extldflags "-static"'
BUILD_LDFLAGS := '$(LDFLAGS)'
TAG := latest

HOST ?= $(shell uname)
CPU ?= $(shell uname -m)

# if no host is identifed (no uname tool)
# we assume a Linux-64bit build
ifeq ($(HOST),)
  HOST = Linux
endif

# identify CPU
ifeq ($(CPU), x86_64)
  HOST := $(HOST)64
else
ifeq ($(CPU), i686)
  HOST := $(HOST)32
endif
endif


#############################################
# now we find out the target OS for
# which we are going to compile in case
# the caller didn't yet define OS himself
ifndef (OS)
  ifeq ($(HOST), Linux64)
    arch = gcc
  else
  ifeq ($(HOST), Linux32)
    arch = 32
  else
  ifeq ($(HOST), Darwin64)
    arch = clang
  else
  ifeq ($(HOST), Darwin32)
    arch = clang
  endif
  endif
  endif
  endif
endif

all: install

checks:
	@echo "Checking deps:"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)
	@(env bash $(PWD)/buildscripts/checkgopath.sh)

checkdocker:
	@echo "Checking if docker is installed.. "
	@if [ -z ${DOCKER_BIN} ]; then echo "Docker not installed, cannot build docker image. Please install 'sudo apt-get install docker.io'" && exit 1; else echo "Docker installed at ${DOCKER_BIN}."; fi;

getdeps: checks
	@go get -u github.com/golang/lint/golint && echo "Installed golint:"
	@go get -u golang.org/x/tools/cmd/vet && echo "Installed vet:"
	@go get -u github.com/fzipp/gocyclo && echo "Installed gocyclo:"
	@go get -u github.com/remyoudompheng/go-misc/deadcode && echo "Installed deadcode:"
	@go get -u github.com/client9/misspell/cmd/misspell && echo "Installed misspell:"

verifiers: getdeps vet fmt lint cyclo spelling

vet:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 go tool vet -all *.go
	@GO15VENDOREXPERIMENT=1 go tool vet -all ./pkg
	@GO15VENDOREXPERIMENT=1 go tool vet -shadow=true *.go
	@GO15VENDOREXPERIMENT=1 go tool vet -shadow=true ./pkg

fmt:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 gofmt -s -l *.go
	@GO15VENDOREXPERIMENT=1 gofmt -s -l pkg

## Configure Intel library.
isa-l:

	@echo "Configuring $@:"
	@git clone -q https://github.com/minio/isa-l.git
	@cd isa-l; make -f Makefile.unx arch=$(arch) >/dev/null; mv include isa-l;

lint:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/golint *.go
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/golint github.com/minio/minio/pkg...

cyclo:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/gocyclo -over 65 *.go
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/gocyclo -over 65 pkg

build: getdeps verifiers $(UI_ASSETS) isa-l
	@GO15VENDOREXPERIMENT=1 go generate github.com/minio/minio/pkg/crypto/sha1

deadcode:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/deadcode

spelling:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/misspell *.go
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/misspell pkg/**/*

test: build
	@echo "Running all minio testing:"
	@GODEBUG=cgocheck=0 CGO_CPPFLAGS="-I$(PWD)/isa-l" CGO_LDFLAGS="$(PWD)/isa-l/isa-l.a" GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) .
	@GODEBUG=cgocheck=0 CGO_CPPFLAGS="-I$(PWD)/isa-l" CGO_LDFLAGS="$(PWD)/isa-l/isa-l.a" GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) github.com/minio/minio/pkg...

gomake-all: build
	@echo "Installing minio:"
	@CGO_CPPFLAGS="-I$(PWD)/isa-l" CGO_LDFLAGS="$(PWD)/isa-l/isa-l.a" GO15VENDOREXPERIMENT=1 go build --ldflags $(BUILD_LDFLAGS) -o $(GOPATH)/bin/minio

pkg-add:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor add $(PKG)

pkg-update:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor update $(PKG)

pkg-remove:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor remove $(PKG)

pkg-list:
	@GO15VENDOREXPERIMENT=1 $(GOPATH)/bin/govendor list

install: gomake-all

dockerimage: checkdocker verifiers $(UI_ASSETS)
	@echo "Building docker image:" minio:$(TAG)
	@GO15VENDOREXPERIMENT=1 GOOS=linux GOARCH=amd64 go build --ldflags $(DOCKER_LDFLAGS) -o minio.dockerimage
	@mkdir -p export
	@sudo docker build --rm --tag=minio/minio:$(TAG) .
	@rmdir export
	@rm minio.dockerimage

release: verifiers
	@MINIO_RELEASE=RELEASE ./buildscripts/build.sh

experimental: verifiers
	@MINIO_RELEASE=EXPERIMENTAL ./buildscripts/build.sh

clean:
	@echo "Cleaning up all the generated files:"
	@rm -fv minio minio.test cover.out
	@find . -name '*.test' | xargs rm -fv
	@rm -rf isa-l
	@rm -rf build
	@rm -rf release
