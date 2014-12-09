package main

import (
	"bytes"
	"io"
	"os"
	"path"

	"github.com/minio-io/minio/pkgs/storage"
	"github.com/minio-io/minio/pkgs/storage/fsstorage"
)

func fsGetList(config inputConfig) (io.Reader, error) {
	var objectStorage storage.ObjectStorage
	rootDir := path.Join(config.rootDir, config.storageDriver)
	objectStorage = fsstorage.FileSystemStorage{RootDir: rootDir}
	objectlist, err := objectStorage.GetList()
	if err != nil {
		return nil, err
	}
	objectListBuffer := bytes.NewBuffer(objectlist)
	return objectListBuffer, nil
}

func fsGet(config inputConfig, objectPath string) (io.Reader, error) {
	var objectStorage storage.ObjectStorage
	rootDir := path.Join(config.rootDir, config.storageDriver)
	objectStorage = fsstorage.FileSystemStorage{RootDir: rootDir}
	object, err := objectStorage.Get(objectPath)
	if err != nil {
		return nil, err
	}
	objectBuffer := bytes.NewBuffer(object)
	return objectBuffer, nil
}

func fsPut(config inputConfig, objectPath string, reader io.Reader) error {
	var err error
	rootDir := path.Join(config.rootDir, config.storageDriver)
	if err := os.MkdirAll(config.rootDir, 0700); err != nil {
		return err
	}
	var objectStorage storage.ObjectStorage
	buffer := new(bytes.Buffer)
	buffer.ReadFrom(reader)
	object := buffer.Bytes()
	objectStorage = fsstorage.FileSystemStorage{RootDir: rootDir}
	if err = objectStorage.Put(objectPath, object); err != nil {
		return err
	}
	return nil
}
