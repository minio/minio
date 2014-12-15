package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path"

	"github.com/minio-io/minio/pkgs/storage"
	es "github.com/minio-io/minio/pkgs/storage/encodedstorage"
)

func erasureGetList(config inputConfig, objectPath string) (io.Reader, error) {
	var objectStorage storage.ObjectStorage
	rootDir := path.Join(config.rootDir, config.storageDriver)
	objectStorage, err := es.NewStorage(rootDir, config.k, config.m, config.blockSize)
	if err != nil {
		return nil, err
	}
	objectDescList, err := objectStorage.List(objectPath)
	if err != nil {
		return nil, err
	}
	var objectDescListBytes []byte
	if objectDescListBytes, err = json.Marshal(objectDescList); err != nil {
		return nil, err
	}
	objectDescListBuffer := bytes.NewBuffer(objectDescListBytes)

	return objectDescListBuffer, nil
}

func erasureGet(config inputConfig, objectPath string) (io.Reader, error) {
	var objectStorage storage.ObjectStorage
	rootDir := path.Join(config.rootDir, config.storageDriver)
	objectStorage, err := es.NewStorage(rootDir, config.k, config.m, config.blockSize)
	if err != nil {
		return nil, err
	}
	object, err := objectStorage.Get(objectPath)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func erasurePut(config inputConfig, objectPath string, reader io.Reader) error {
	var err error
	rootDir := path.Join(config.rootDir, config.storageDriver)
	if err := os.MkdirAll(rootDir, 0700); err != nil {
		return err
	}
	var objectStorage storage.ObjectStorage
	if objectStorage, err = es.NewStorage(rootDir, config.k, config.m, config.blockSize); err != nil {
		return err
	}
	if err = objectStorage.Put(objectPath, reader); err != nil {
		return err
	}
	return nil
}
