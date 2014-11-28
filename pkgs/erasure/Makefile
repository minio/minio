all: build test
.PHONY: all

test:
	@godep go test

isal/isal-l.so:
	@$(MAKE) --quiet -C isal lib

build: isal/isal-l.so
	@godep go build
