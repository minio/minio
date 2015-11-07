LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)
DOCKER_LDFLAGS := $(LDFLAGS) -extldflags "-static"
TAG := latest

all: install

checkdeps:
	@echo "Checking deps:"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

checkgopath:
	@echo "Checking if project is at ${GOPATH}"
	@for miniopath in $(echo ${GOPATH} | sed 's/:/\n/g'); do if [ ! -d ${miniopath}/src/github.com/minio/minio ]; then echo "Project not found in ${miniopath}, please follow instructions provided at https://github.com/minio/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" && exit 1; fi done
	@echo "Setting LDFLAGS: ${LDFLAGS}"

getdeps: checkdeps checkgopath
	@go get -u github.com/golang/lint/golint && echo "Installed golint:"
	@go get -u golang.org/x/tools/cmd/vet && echo "Installed vet:"
	@go get -u github.com/fzipp/gocyclo && echo "Installed gocyclo:"
	@go get -u github.com/remyoudompheng/go-misc/deadcode && echo "Installed deadcode:"

verifiers: getdeps vet fmt lint cyclo

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
	@GO15VENDOREXPERIMENT=1 golint *.go
	@GO15VENDOREXPERIMENT=1 golint github.com/minio/minio/pkg...

cyclo:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 gocyclo -over 65 *.go
	@GO15VENDOREXPERIMENT=1 gocyclo -over 65 pkg

build: getdeps verifiers
	@echo "Installing minio:"

deadcode:
	@GO15VENDOREXPERIMENT=1 deadcode

test: build
	@echo "Running all testing:"
	@GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) .
	@GO15VENDOREXPERIMENT=1 go test $(GOFLAGS) github.com/minio/minio/pkg...

gomake-all: build
	@GO15VENDOREXPERIMENT=1 go build --ldflags '$(LDFLAGS)' -o $(GOPATH)/bin/minio

pkg-add:
	@GO15VENDOREXPERIMENT=1 govendor add $(PKG)

pkg-update:
	@GO15VENDOREXPERIMENT=1 govendor update $(PKG)

pkg-remove:
	@GO15VENDOREXPERIMENT=1 govendor remove $(PKG)

install: gomake-all

dockerimage: install
	@echo "Building docker image:" minio:$(TAG)
	@GO15VENDOREXPERIMENT=1 go build --ldflags '$(DOCKER_LDFLAGS)' -o minio.dockerimage
	@mkdir -p export
	@docker build --rm --tag=minio:$(TAG) .
	@rmdir export
	@rm minio.dockerimage

clean:
	@echo "Cleaning up all the generated files:"
	@rm -fv cover.out
	@rm -fv minio
	@rm -fv minio.test
	@rm -fv pkg/fs/fs.test
