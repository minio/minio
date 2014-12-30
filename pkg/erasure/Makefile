all: build test
.PHONY: all

test:
	@godep go test -race -coverprofile=cover.out

isal/isal-l.a:
	@$(MAKE) -C isal lib

build: isal/isal-l.a
	@godep go build

clean:
	@rm -v cover.out
