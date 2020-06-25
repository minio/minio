GOPATH := $(shell go env GOPATH)

all: check

getdeps:
	@if [ ! -f ${GOPATH}/bin/golint ]; then echo "Installing golint" && go get -u golang.org/x/lint/golint; fi
	@if [ ! -f ${GOPATH}/bin/gocyclo ]; then echo "Installing gocyclo" && go get -u github.com/fzipp/gocyclo; fi
	@if [ ! -f ${GOPATH}/bin/misspell ]; then echo "Installing misspell" && go get -u github.com/client9/misspell/cmd/misspell; fi
	@if [ ! -f ${GOPATH}/bin/ineffassign ]; then echo "Installing ineffassign" && go get -u github.com/gordonklaus/ineffassign; fi

vet:
	@echo "Running $@"
	@go vet *.go

fmt:
	@echo "Running $@"
	@gofmt -d *.go

lint:
	@echo "Running $@"
	@${GOPATH}/bin/golint -set_exit_status

cyclo:
	@echo "Running $@"
	@${GOPATH}/bin/gocyclo -over 200 .

spelling:
	@${GOPATH}/bin/misspell -locale US -error *.go README.md

ineffassign:
	@echo "Running $@"
	@${GOPATH}/bin/ineffassign .

check: getdeps vet fmt lint cyclo spelling ineffassign
	@echo "Running unit tests"
	@go test -tags kqueue ./...
