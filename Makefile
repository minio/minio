#GOPATH := $(CURDIR)/tmp/gopath
MAKE_OPTIONS := -s
ARCH := $(shell uname -s)

all: getdeps install

checkdeps:
	@./checkdeps.sh

getdeps: checkdeps
	@go get github.com/tools/godep && echo "Installed godep"
	@go get golang.org/x/tools/cmd/cover && echo "Installed cover"

build-utils:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/cpu
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/unitconv
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/split
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/crypto/md5
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/crypto/sha1
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/crypto/sha256
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/crypto/sha512
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/utils/checksum/crc32c

#build-os:
#	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/os/scsi
#	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/os/sysctl

build-storage:
	@godep go generate github.com/minio-io/minio/pkg/storage/erasure
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/storage/erasure

build-minioapi:
	@godep go test -race -coverprofile=cover.out github.com/minio-io/minio/pkg/webapi/minioapi

minio: build-storage build-utils build-minioapi

install: minio
	@godep go install github.com/minio-io/minio && echo "Installed minio"

save: restore
	@godep save ./...

restore:
	@godep restore

env:
	@godep go env

clean:
	@echo "Cleaning up all the generated files"
	@$(MAKE) $(MAKE_OPTIONS) -C pkg/storage/erasure/isal clean
	@rm -fv pkg/utils/split/TESTPREFIX.*
	@rm -fv cover.out
