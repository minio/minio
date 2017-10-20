all: install

REBUILD:
	@touch debug*.go

dependencies:
	go get -u github.com/dvyukov/go-fuzz/go-fuzz
	go get -u github.com/dvyukov/go-fuzz/go-fuzz-build

install: REBUILD
	go install

test: REBUILD
	go test -v -tags='debug' -timeout=600s
test-short: REBUILD
	go test -short -v -tags='debug' -timeout=6s

cover: REBUILD
	go test -v -tags='debug' -cover -coverprofile=cover.out
	go tool cover -html=cover.out -o=cover.html
	rm cover.out

fuzz: REBUILD
	go install -tags='debug gofuzz'
	go-fuzz-build github.com/NebulousLabs/merkletree
	go-fuzz -bin=./merkletree-fuzz.zip -workdir=fuzz

.PHONY: all REBUILD dependencies install test test-short cover fuzz benchmark
