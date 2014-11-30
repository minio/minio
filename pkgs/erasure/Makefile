all: build test
.PHONY: all

test:
	@godep go test

isal/isal-l.a:
	@$(MAKE) --quiet -C isal lib

build: isal/isal-l.a
	@godep go build
