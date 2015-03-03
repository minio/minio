MINIOPATH=$(GOPATH)/src/github.com/minio-io/minio

all: getdeps install

checkdeps:
	@echo "Checking deps.."
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

checkgopath:
	@echo "Checking project in ${MINIOPATH}"
	@if [ ! -d ${MINIOPATH} ]; then echo "Project not found in $GOPATH, please follow instructions provided at https://github.com/Minio-io/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" && exit 1; fi

getdeps: checkdeps checkgopath
	@go get github.com/tools/godep && echo "Installed godep"

deadcode: getdeps
	@go run buildscripts/deadcode.go .

build-all: getdeps deadcode
	@echo "Building Libraries"
	@godep go generate ./...
	@godep go build ./...

test-all: build-all
	@echo "Running Test Suites:"
	@godep go test -race ./...

test: test-all

minio: build-all test-all

install: minio
	@godep go install github.com/minio-io/minio && echo "Installed minio"

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

docs-deploy:
	@mkdocs gh-deploy --clean

clean:
	@echo "Cleaning up all the generated files"
	@rm -fv pkg/utils/split/TESTPREFIX.*
	@rm -fv cover.out
	@rm -fv pkg/storage/erasure/*.syso
