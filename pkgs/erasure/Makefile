all: build test
.PHONY: all

test: test-race test-cover

test-race:
	@godep go test -race

test-cover:
	@godep go test -coverprofile=cover.out

isal/isal-l.a:
	@$(MAKE) -C isal lib

build: isal/isal-l.a
	@godep go build

clean:
	@rm -v cover.out
