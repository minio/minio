all: build test
.PHONY: all

test: cauchy vandermonde

cauchy:
	@go test -test.run="TestCauchy*"

vandermonde:
	@go test -test.run="TestVanderMonde*"

isal/isal-l.so:
	@$(MAKE) --quiet -C isal

build: isal/isal-l.so
	@go build
