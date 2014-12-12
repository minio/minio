package fsstorage

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/minio-io/minio/pkgs/storage"
)

type fileSystemStorage struct {
	RootDir string
}

func NewStorage(rootDir string) (storage.ObjectStorage, error) {
	newStorage := fileSystemStorage{
		RootDir: rootDir,
	}
	return &newStorage, nil
}

func (fsStorage *fileSystemStorage) List() ([]storage.ObjectDescription, error) {
	fileInfos, err := ioutil.ReadDir(fsStorage.RootDir)
	if err != nil {
		return nil, err
	}

	var descriptions []storage.ObjectDescription

	for _, fi := range fileInfos {
		description := storage.ObjectDescription{
			Name:            fi.Name(),
			Md5sum:          "",
			Protectionlevel: "",
		}
		descriptions = append(descriptions, description)
	}
	return descriptions, nil
}

func (storage *fileSystemStorage) Get(objectPath string) (io.Reader, error) {
	return os.Open(path.Join(storage.RootDir, objectPath))
}

func (storage *fileSystemStorage) Put(objectPath string, object io.Reader) error {
	err := os.MkdirAll(filepath.Dir(path.Join(storage.RootDir, objectPath)), 0700)
	if err != nil {
		return err
	}
	objectBytes, err := ioutil.ReadAll(object)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path.Join(storage.RootDir, objectPath), objectBytes, 0600)
}
