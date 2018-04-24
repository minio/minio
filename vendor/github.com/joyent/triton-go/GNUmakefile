TEST?=$$(go list ./... |grep -Ev 'vendor|examples|testutils')
GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)

default: check test

tools:: ## Download and install all dev/code tools
	@echo "==> Installing dev tools"
	go get -u github.com/golang/dep/cmd/dep
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install

test:: ## Run unit tests
	@echo "==> Running unit test with coverage"
	@./scripts/go-test-with-coverage.sh

testacc:: ## Run acceptance tests
	@echo "==> Running acceptance tests"
	TRITON_TEST=1 go test $(TEST) -v $(TESTARGS) -run -timeout 120m

check::
	gometalinter \
    		--deadline 10m \
    		--vendor \
    		--sort="path" \
    		--aggregate \
    		--enable-gc \
    		--disable-all \
    		--enable goimports \
    		--enable misspell \
    		--enable vet \
    		--enable deadcode \
    		--enable varcheck \
    		--enable ineffassign \
    		--enable gofmt \
    		./...

.PHONY: help
help:: ## Display this help message
	@echo "GNU make(1) targets:"
	@grep -E '^[a-zA-Z_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
