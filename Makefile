all: install

checkdeps:
	@echo "Checking deps:"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

checkgopath:
	@echo "Checking if project is at ${GOPATH}"
	@for mcpath in $(echo ${GOPATH} | sed 's/:/\n/g'); do if [ ! -d ${mcpath}/src/github.com/minio/minio ]; then echo "Project not found in ${mcpath}, please follow instructions provided at https://github.com/minio/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" && exit 1; fi done

getdeps: checkdeps checkgopath
	@go get github.com/golang/lint/golint && echo "Installed golint:"
	@go get golang.org/x/tools/cmd/vet && echo "Installed vet:"
	@go get github.com/fzipp/gocyclo && echo "Installed gocyclo:"

verifiers: getdeps vet fmt lint cyclo

vet:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 go vet .
	@GO15VENDOREXPERIMENT=1 go vet github.com/minio/minio/pkg...

fmt:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 gofmt -s -l *.go
	@GO15VENDOREXPERIMENT=1 gofmt -s -l pkg

lint:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 golint .
	@GO15VENDOREXPERIMENT=1 golint pkg

cyclo:
	@echo "Running $@:"
	@GO15VENDOREXPERIMENT=1 gocyclo -over 25 .

build: getdeps verifiers
	@echo "Installing minio:"
	@GO15VENDOREXPERIMENT=1 go generate ./...
	@GO15VENDOREXPERIMENT=1 go test -race github.com/minio/minio/pkg...

gomake-all: build
	@GO15VENDOREXPERIMENT=1 go install github.com/minio/minio

release: genversion
	@echo "Installing minio for new version.go:"
	@GO15VENDOREXPERIMENT=1 go install github.com/minio/minio

genversion:
	@echo "Generating new minio version.go"
	@go run genversion.go

install: gomake-all

clean:
	@echo "Cleaning up all the generated files:"
	@rm -fv cover.out
	@rm -fv pkg/utils/split/TESTPREFIX.*
	@rm -fv minio
