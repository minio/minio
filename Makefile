#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s

all: getdeps install

checkdeps:
	@./checkdeps.sh

createsymlink:
	@mkdir -p $(GOPATH)/src/github.com/minio-io/;
	@if test ! -e $(GOPATH)/src/github.com/minio-io/minio; then echo "Creating symlink to $(GOPATH)/src/github.com/minio-io/minio" && ln -s $(PWD) $(GOPATH)/src/github.com/minio-io/minio; fi

getdeps: checkdeps
	@go get github.com/tools/godep && echo "Installed godep"
	@go get golang.org/x/tools/cmd/cover && echo "Installed cover"

build-all: getdeps createsymlink
	@echo "Building Libraries"
	@godep go generate ./...
	@godep go build ./...

test-all: build-all
	@echo "Running Test Suites:"
	@godep go test -race ./...

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
