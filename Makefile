#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s

all: getdeps install

checkdeps:
	@./checkdeps.sh

getdeps: checkdeps
	@go get github.com/tools/godep && echo "Installed godep"
	@go get golang.org/x/tools/cmd/cover && echo "Installed cover"

build-utils:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/cpu
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/unitconv

build-crypto:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/md5/
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/sha1/
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/sha256/
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/sha512/

build-checksum:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/checksum/crc32c

build-os:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/os/scsi
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/os/sysctl

build-fileutils: build-utils
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/fileutils/split

build-storage: build-erasure build-storage-append build-storage-encoded

build-erasure:
	@$(MAKE) $(MAKE_OPTIONS) -C pkg/storage/erasure/isal lib
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/storage/erasure

build-storage-append:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/storage/appendstorage

build-storage-encoded:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/storage/encodedstorage

cover: build-fileutils build-checksum build-os build-storage build-crypto

install: cover

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

clean:
	@rm -v cover.out
