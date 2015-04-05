MINIOPATH=$(GOPATH)/src/github.com/minio-io/donut

all: getdeps install

checkdeps:
	@echo "Checking deps:"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

checkgopath:
	@echo "Checking if project is at ${MINIOPATH}"
	@if [ ! -d ${MINIOPATH} ]; then echo "Project not found in $GOPATH, please follow instructions provided at https://github.com/Minio-io/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" && exit 1; fi

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
	@test -z "$$(gocyclo -over 15 . | grep -v Godeps/_workspace/src/ | tee /dev/stderr)"

pre-build:
	@echo "Running pre-build:"
	@(env bash $(PWD)/buildscripts/git-commit-id.sh)

build-all: getdeps verifiers
	@echo "Building Libraries:"
	@godep go generate github.com/minio-io/erasure
	@godep go generate ./...
	@godep go build -a ./... # have no stale packages

test-all: pre-build build-all
	@echo "Running Test Suites:"
	@godep go test -race ./...

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

docs-deploy:
	@mkdocs gh-deploy --clean

install: test-all
	@echo "Installing donut-cli:"
	@godep go install -a github.com/minio-io/donut/cmd/donut-cli
	@mkdir -p $(HOME)/.minio/donut

clean:
	@rm -fv cover.out
	@rm -fv build-constants.go
