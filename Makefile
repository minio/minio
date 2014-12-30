#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s

all: getdeps cover install

checkdeps:
	@./checkdeps.sh

getdeps: checkdeps
	@go get github.com/tools/godep && echo "Installed godep"
	@go get golang.org/x/tools/cmd/cover && echo "Installed cover"

build-erasure:
	@$(MAKE) $(MAKE_OPTIONS) -C pkg/erasure/isal lib
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/erasure

build-signify:
	@$(MAKE) $(MAKE_OPTIONS) -C pkg/signify

build-cpu:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/cpu

build-md5:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/md5/
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/md5c/

build-sha1:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/sha1/

build-sha256:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/sha256/

build-sha512:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/crypto/sha512/

build-crc32c:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/checksum/crc32c

build-scsi:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/scsi

build-split: build-strbyteconv
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/split

build-strbyteconv:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/strbyteconv

build-storage: build-storage-append build-storage-encoded

build-storage-append:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/storage/appendstorage

build-storage-encoded:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/storage/encodedstorage

cover: build-erasure build-signify build-split build-crc32c build-cpu build-scsi build-storage build-md5 build-sha1 build-sha256 build-sha512
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/gateway

install: build-erasure
	@godep go install github.com/minio-io/minio/cmd/minio && echo "Installed minio into ${GOPATH}/bin"
	@godep go install github.com/minio-io/minio/cmd/new-cmd && echo "Installed new-cmd into ${GOPATH}/bin"
	@godep go install github.com/minio-io/minio/cmd/crypto && echo "Installed crypto into ${GOPATH}/bin"

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

clean:
	@rm -v cover.out
