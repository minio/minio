all: build test
.PHONY: all

test: cauchy vandermonde

cauchy:
	@godep go test -test.run="TestCauchy*"

vandermonde:
	@godep go test -test.run="TestVanderMonde*"

isal/isal-l.so:
	@$(MAKE) --quiet -C isal lib

build: isal/isal-l.so
	@godep go build
