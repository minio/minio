LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)
DOCKER_BIN := $(shell which docker)
DOCKER_LDFLAGS := '$(LDFLAGS) -extldflags "-static"'
BUILD_LDFLAGS := '$(LDFLAGS)'
TAG := latest
UI_ASSETS := ui-assets.go
UI_ASSETS_ARMOR := ui-assets.asc

all: install

checkdeps:
	@echo "Checking deps:"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

checkgopath:
	@echo "Checking if project is at ${GOPATH}"
	@for miniopath in $(echo ${GOPATH} | sed 's/:/\n/g'); do if [ ! -d ${miniopath}/src/github.com/minio/minio ]; then echo "Project not found in ${miniopath}, please follow instructions provided at https://github.com/minio/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" && exit 1; fi done
	@echo "Setting BUILD_LDFLAGS: ${BUILD_LDFLAGS}"

checkdocker:
	@echo "Checking if docker is installed.. "
	@if [ -z ${DOCKER_BIN} ]; then echo "Docker not installed, cannot build docker image. Please install 'sudo apt-get install docker.io'" && exit 1; else echo "Docker installed at ${DOCKER_BIN}."; fi;

getdeps: checkdeps checkgopath
	@go get -u github.com/golang/lint/golint && echo "Installed golint:"
	@go get -u golang.org/x/tools/cmd/vet && echo "Installed vet:"
	@go get -u github.com/fzipp/gocyclo && echo "Installed gocyclo:"
	@go get -u github.com/remyoudompheng/go-misc/deadcode && echo "Installed deadcode:"
	@go get -u github.com/client9/misspell/cmd/misspell && echo "Installed misspell:"

$(UI_ASSETS):
	@curl -s https://dl.minio.io/assets/server/$(UI_ASSETS_ARMOR) 2>&1 > $(UI_ASSETS_ARMOR) && echo "Downloading signature file $(UI_ASSETS_ARMOR) for verification:"
	@gpg --batch --no-tty --yes --keyserver pgp.mit.edu --recv-keys F9AAC728 2>&1 > /dev/null && echo "Importing public key:"
	@curl -s https://dl.minio.io/assets/server/$@ 2>&1 > $@ && echo "Downloading UI assets file $@:"
	@gpg --batch --no-tty --verify $(UI_ASSETS_ARMOR) $@ 2>&1 > /dev/null && echo "Verifying signature of downloaded assets."

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

lint:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/golint *.go
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/golint github.com/minio/minio/pkg...

cyclo:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/gocyclo -over 65 *.go
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/gocyclo -over 65 pkg

build: getdeps verifiers $(UI_ASSETS)
	@echo "Installing minio:"

deadcode:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/deadcode

spelling:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/misspell *.go
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/misspell pkg/**/*

test: build
	@echo "Running all testing:"
	@GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) .
	@GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) github.com/minio/minio/pkg...

gomake-all: build
	@GO15VENDOREXPERIMENT=1 go build --ldflags $(BUILD_LDFLAGS) -o $(GOPATH)/bin/minio

pkg-add:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor add $(PKG)

pkg-update:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor update $(PKG)

pkg-remove:
	@GO15VENDOREXPERIMENT=1 ${GOPATH}/bin/govendor remove $(PKG)

install: gomake-all

dockerimage: checkdocker verifiers $(UI_ASSETS)
	@echo "Building docker image:" minio:$(TAG)
	@GO15VENDOREXPERIMENT=1 go build --ldflags $(DOCKER_LDFLAGS) -o minio.dockerimage
	@mkdir -p export
	@sudo docker build --rm --tag=minio/minio:$(TAG) .
	@rmdir export
	@rm minio.dockerimage

release:
	@./release.sh

clean:
	@echo "Cleaning up all the generated files:"
	@rm -fv cover.out
	@rm -fv minio
	@rm -fv minio.test
	@rm -fv pkg/fs/fs.test
	@rm -fv ui-assets.go
	@rm -fv ui-assets.asc
