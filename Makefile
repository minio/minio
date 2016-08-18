LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)
PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
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
ifeq ($(CPU), amd64)
  HOST := $(HOST)64
else
ifeq ($(CPU), i686)
  HOST := $(HOST)32
endif
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
  else
  ifeq ($(HOST), FreeBSD64)
    arch = gcc
  endif
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

getdeps: checks
	@go get -u github.com/golang/lint/golint && echo "Installed golint:"
	@go get -u github.com/fzipp/gocyclo && echo "Installed gocyclo:"
	@go get -u github.com/remyoudompheng/go-misc/deadcode && echo "Installed deadcode:"
	@go get -u github.com/client9/misspell/cmd/misspell && echo "Installed misspell:"
	@go get -u github.com/gordonklaus/ineffassign && echo "Installed ineffassign:"

verifiers: vet fmt lint cyclo spelling

vet:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 go tool vet -all ./cmd
	@GO15VENDOREXPERIMENT=1 go tool vet -all ./pkg
	@GO15VENDOREXPERIMENT=1 go tool vet -shadow=true ./cmd
	@GO15VENDOREXPERIMENT=1 go tool vet -shadow=true ./pkg

fmt:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 gofmt -s -l cmd
	@GO15VENDOREXPERIMENT=1 gofmt -s -l pkg

lint:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/golint github.com/minio/minio/cmd...
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/golint github.com/minio/minio/pkg...

ineffassign:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/ineffassign .

cyclo:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/gocyclo -over 65 cmd
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/gocyclo -over 65 pkg

build: getdeps verifiers $(UI_ASSETS)

deadcode:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/deadcode

spelling:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/misspell -error cmd/**/*
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/misspell -error pkg/**/*

test: build
	@echo "Running all minio testing:"
	@GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) github.com/minio/minio/cmd...
	@GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) github.com/minio/minio/pkg...

coverage: build
	@echo "Running all coverage for minio:"
	@GO15VENDOREXPERIMENT=1 ./buildscripts/go-coverage.sh

gomake-all: build
	@echo "Installing minio:"
	@GO15VENDOREXPERIMENT=1 go build --ldflags $(BUILD_LDFLAGS) -o $(GOPATH)/bin/minio

pkg-add:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor add $(PKG)

pkg-update:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor update $(PKG)

pkg-remove:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor remove $(PKG)

pkg-list:
	@GO15VENDOREXPERIMENT=1 $(GOPATH)/bin/govendor list

install: gomake-all

release: verifiers
	@MINIO_RELEASE=RELEASE ./buildscripts/build.sh

experimental: verifiers
	@MINIO_RELEASE=EXPERIMENTAL ./buildscripts/build.sh

clean:
	@echo "Cleaning up all the generated files:"
	@find . -name '*.test' | xargs rm -fv
	@rm -rf build
	@rm -rf release
