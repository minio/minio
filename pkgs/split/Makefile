all: build test
.PHONY: all

build:
	@godep go build

test: build
	@godep go test -race -coverprofile=cover.out

clean:
	@rm -fv NEWFILE TESTPREFIX.* cover.out
