PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)
LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)

BUILD_LDFLAGS := '$(LDFLAGS)'

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)
	@echo "Checking for project in GOPATH"
	@(env bash $(PWD)/buildscripts/checkgopath.sh)

getdeps:
	@mkdir -p $(GOPATH)/bin
	@echo "Installing golint" && which golint || go get -u golang.org/x/lint/golint
	@echo "Installing staticcheck" && which staticcheck || wget --quiet -O $(GOPATH)/bin/staticcheck https://github.com/dominikh/go-tools/releases/download/2019.1/staticcheck_linux_amd64 && chmod +x $(GOPATH)/bin/staticcheck
	@echo "Installing misspell" && which misspell || wget --quiet https://github.com/client9/misspell/releases/download/v0.3.4/misspell_0.3.4_linux_64bit.tar.gz && tar xvf misspell_0.3.4_linux_64bit.tar.gz && mv misspell $(GOPATH)/bin/misspell && chmod +x $(GOPATH)/bin/misspell && rm -r misspell_0.3.4_linux_64bit.tar.gz

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps vet fmt lint staticcheck spelling

vet:
	@echo "Running $@"
	@go vet github.com/minio/minio/...

fmt:
	@echo "Running $@"
	@gofmt -d cmd/
	@gofmt -d pkg/

lint:
	@echo "Running $@"
	@${GOPATH}/bin/golint -set_exit_status github.com/minio/minio/cmd/...
	@${GOPATH}/bin/golint -set_exit_status github.com/minio/minio/pkg/...

staticcheck:
	@echo "Running $@"
	@${GOPATH}/bin/staticcheck github.com/minio/minio/cmd/...
	@${GOPATH}/bin/staticcheck github.com/minio/minio/pkg/...

spelling:
	@${GOPATH}/bin/misspell -locale US -error `find cmd/`
	@${GOPATH}/bin/misspell -locale US -error `find pkg/`
	@${GOPATH}/bin/misspell -locale US -error `find docs/`
	@${GOPATH}/bin/misspell -locale US -error `find buildscripts/`
	@${GOPATH}/bin/misspell -locale US -error `find dockerscripts/`

# Builds minio, runs the verifiers then runs the tests.
check: test
test: verifiers build
	@echo "Running unit tests"
	@CGO_ENABLED=0 go test -tags kqueue ./...

verify: build
	@echo "Verifying build"
	@(env bash $(PWD)/buildscripts/verify-build.sh)

coverage: build
	@echo "Running all coverage for minio"
	@(env bash $(PWD)/buildscripts/go-coverage.sh)

# Builds minio locally.
build: checks
	@echo "Building minio binary to './minio'"
	@GOFLAGS="" CGO_ENABLED=0 go build -tags kqueue --ldflags $(BUILD_LDFLAGS) -o $(PWD)/minio
	@GOFLAGS="" CGO_ENABLED=0 go build -tags kqueue --ldflags="-s -w" -o $(PWD)/dockerscripts/healthcheck $(PWD)/dockerscripts/healthcheck.go

docker: build
	@docker build -t $(TAG) . -f Dockerfile.dev

pkg-add:
	@echo "Adding new package $(PKG)"
	@${GOPATH}/bin/govendor add $(PKG)

pkg-update:
	@echo "Updating new package $(PKG)"
	@${GOPATH}/bin/govendor update $(PKG)

pkg-remove:
	@echo "Remove new package $(PKG)"
	@${GOPATH}/bin/govendor remove $(PKG)

pkg-list:
	@$(GOPATH)/bin/govendor list

# Builds minio and installs it to $GOPATH/bin.
install: build
	@echo "Installing minio binary to '$(GOPATH)/bin/minio'"
	@mkdir -p $(GOPATH)/bin && cp $(PWD)/minio $(GOPATH)/bin/minio
	@echo "Installation successful. To learn more, try \"minio --help\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf minio
	@rm -rvf build
	@rm -rvf release
