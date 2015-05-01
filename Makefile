all: getdeps install

checkdeps:
	@echo "Checking deps:"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

checkgopath:
	@echo "Checking if project is at ${GOPATH}"
	@for mcpath in $(echo ${GOPATH} | sed 's/:/\n/g' | grep -v Godeps); do if [ ! -d ${mcpath}/src/github.com/minio-io/minio ]; then echo "Project not found in ${mcpath}, please follow instructions provided at https://github.com/Minio-io/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" && exit 1; fi done

getdeps: checkdeps checkgopath
	@go get github.com/minio-io/godep && echo "Installed godep:"
	@go get github.com/golang/lint/golint && echo "Installed golint:"
	@go get golang.org/x/tools/cmd/vet && echo "Installed vet:"
	@go get github.com/fzipp/gocyclo && echo "Installed gocyclo:"

verifiers: getdeps vet fmt lint cyclo

vet:
	@echo "Running $@:"
	@go vet ./...
fmt:
	@echo "Running $@:"
	@test -z "$$(gofmt -s -l . | grep -v Godeps/_workspace/src/ | tee /dev/stderr)" || \
		echo "+ please format Go code with 'gofmt -s'"
lint:
	@echo "Running $@:"
	@test -z "$$(golint ./... | grep -v Godeps/_workspace/src/ | tee /dev/stderr)"

cyclo:
	@echo "Running $@:"
	@test -z "$$(gocyclo -over 18 . | grep -v Godeps/_workspace/src/ | tee /dev/stderr)"

pre-build:
	@echo "Running pre-build:"

build-all: verifiers
	@echo "Building Libraries:"
	@godep go generate ./...
	@godep go build -a ./... # have no stale packages

test-all: build-all
	@echo "Running Test Suites:"
	@godep go test -race ./...

test: test-all

minio: pre-build build-all test-all

install: minio
	@echo "Installing minio:"
	@godep go install -a -ldflags "-X main.BuildDate `date --universal '+%FT%T.%N%:z'`" github.com/minio-io/minio

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

clean:
	@echo "Cleaning up all the generated files:"
	@rm -fv cover.out
	@rm -fv pkg/utils/split/TESTPREFIX.*
	@rm -fv minio
