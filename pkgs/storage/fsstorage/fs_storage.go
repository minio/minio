package fsstorage

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/minio-io/minio/pkgs/storage"
)

type FileSystemStorage struct {
	RootDir string
}

func (fsStorage FileSystemStorage) List(listPath string) ([]storage.ObjectDescription, error) {
	fileInfos, err := ioutil.ReadDir(path.Join(fsStorage.RootDir, listPath))
	if err != nil {
		return nil, err
	}

	var descriptions []storage.ObjectDescription

	for _, fi := range fileInfos {
		description := storage.ObjectDescription{
			Path:  fi.Name(),
			IsDir: fi.IsDir(),
			Hash:  "", // TODO
		}
		descriptions = append(descriptions, description)
	}
	return descriptions, nil
}

func (storage FileSystemStorage) Get(objectPath string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(storage.RootDir, objectPath))
}

func (storage FileSystemStorage) Put(objectPath string, object []byte) error {
	err := os.MkdirAll(filepath.Dir(path.Join(storage.RootDir, objectPath)), 0700)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path.Join(storage.RootDir, objectPath), object, 0600)
}
