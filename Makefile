PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
TAG ?= "minio/minio:$(VERSION)"

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

getdeps:
	@mkdir -p ${GOPATH}/bin
	@echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.40.1
	@which msgp 1>/dev/null || (echo "Installing msgp" && go install -v github.com/tinylib/msgp@v1.1.3)
	@which stringer 1>/dev/null || (echo "Installing stringer" && go install -v golang.org/x/tools/cmd/stringer)

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps lint check-gen

check-gen:
	@go generate ./... >/dev/null
	@(! git diff --name-only | grep '_gen.go$$') || (echo "Non-committed changes in auto-generated code is detected, please commit them to proceed." && false)

lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --build-tags kqueue --timeout=10m --config ./.golangci.yml

# Builds minio, runs the verifiers then runs the tests.
check: test
test: verifiers build
	@echo "Running unit tests"
	@GOGC=25 GO111MODULE=on CGO_ENABLED=0 go test -tags kqueue ./... 1>/dev/null

test-race: verifiers build
	@echo "Running unit tests under -race"
	@(env bash $(PWD)/buildscripts/race.sh)

# Verify minio binary
verify:
	@echo "Verifying build with race"
	@GO111MODULE=on CGO_ENABLED=1 go build -race -tags kqueue -trimpath --ldflags "$(LDFLAGS)" -o $(PWD)/minio 1>/dev/null
	@(env bash $(PWD)/buildscripts/verify-build.sh)

# Verify healing of disks with minio binary
verify-healing:
	@echo "Verify healing build with race"
	@GO111MODULE=on CGO_ENABLED=1 go build -race -tags kqueue -trimpath --ldflags "$(LDFLAGS)" -o $(PWD)/minio 1>/dev/null
	@(env bash $(PWD)/buildscripts/verify-healing.sh)

# Builds minio locally.
build: checks
	@echo "Building minio binary to './minio'"
	@GO111MODULE=on CGO_ENABLED=0 go build -tags kqueue -trimpath --ldflags "$(LDFLAGS)" -o $(PWD)/minio 1>/dev/null

hotfix-vars:
	$(eval LDFLAGS := $(shell MINIO_RELEASE="RELEASE" MINIO_HOTFIX="hotfix.$(shell git rev-parse --short HEAD)" go run buildscripts/gen-ldflags.go $(shell git describe --tags --abbrev=0 | \
    sed 's#RELEASE\.\([0-9]\+\)-\([0-9]\+\)-\([0-9]\+\)T\([0-9]\+\)-\([0-9]\+\)-\([0-9]\+\)Z#\1-\2-\3T\4:\5:\6Z#')))
	$(eval TAG := "minio/minio:$(shell git describe --tags --abbrev=0).hotfix.$(shell git rev-parse --short HEAD)")
hotfix: hotfix-vars install

docker-hotfix: hotfix checks
	@echo "Building minio docker image '$(TAG)'"
	@docker build -t $(TAG) . -f Dockerfile.dev

docker: build checks
	@echo "Building minio docker image '$(TAG)'"
	@docker build -t $(TAG) . -f Dockerfile.dev

# Builds minio and installs it to $GOPATH/bin.
install: build
	@echo "Installing minio binary to '$(GOPATH)/bin/minio'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/minio $(GOPATH)/bin/minio
	@echo "Installation successful. To learn more, try \"minio --help\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf minio
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*
