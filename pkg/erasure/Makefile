all: build test
.PHONY: all

SYSTEM_NAME := $(shell uname -s)

test:
	@godep go test -race -coverprofile=cover.out

isal/isal-l.a:
ifeq ($(SYSTEM_NAME), Darwin)
	@$(MAKE) -C isal arch=osx lib
else
	@$(MAKE) -C isal lib
endif
build: isal/isal-l.a
	@godep go build

clean:
	@rm -v cover.out
