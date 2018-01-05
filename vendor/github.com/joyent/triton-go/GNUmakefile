TEST?=$$(go list ./... |grep -Ev 'vendor|examples|testutils')
GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)

default: vet errcheck test

tools:: ## Download and install all dev/code tools
	@echo "==> Installing dev tools"
	go get -u github.com/golang/dep/cmd/dep
	go get -u github.com/golang/lint/golint
	go get -u github.com/kisielk/errcheck
	@echo "==> Installing test package dependencies"
	go test -i $(TEST) || exit 1

test:: ## Run unit tests
	@echo "==> Running unit tests"
	@echo $(TEST) | \
		xargs -t go test -v $(TESTARGS) -timeout=30s -parallel=1 | grep -Ev 'TRITON_TEST|TestAcc'

testacc:: ## Run acceptance tests
	@echo "==> Running acceptance tests"
	TRITON_TEST=1 go test $(TEST) -v $(TESTARGS) -run -timeout 120m

vet:: ## Check for unwanted code constructs
	@echo "go vet ."
	@go vet $$(go list ./... | grep -v vendor/) ; if [ $$? -eq 1 ]; then \
		echo ""; \
		echo "Vet found suspicious constructs. Please check the reported constructs"; \
		echo "and fix them if necessary before submitting the code for review."; \
		exit 1; \
	fi

lint:: ## Lint and vet code by common Go standards
	@bash $(CURDIR)/scripts/lint.sh

fmt:: ## Format as canonical Go code
	gofmt -w $(GOFMT_FILES)

fmtcheck:: ## Check if code format is canonical Go
	@bash $(CURDIR)/scripts/gofmtcheck.sh

errcheck:: ## Check for unhandled errors
	@bash $(CURDIR)/scripts/errcheck.sh

.PHONY: help
help:: ## Display this help message
	@echo "GNU make(1) targets:"
	@grep -E '^[a-zA-Z_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
