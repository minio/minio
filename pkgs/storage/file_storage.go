package storage

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type FileStorage struct {
	RootDir string
}

func (storage FileStorage) Get(objectPath string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(storage.RootDir, objectPath))

}

func (storage FileStorage) Put(objectPath string, object []byte) error {
	os.MkdirAll(filepath.Dir(path.Join(storage.RootDir, objectPath)), 0700)
	return ioutil.WriteFile(path.Join(storage.RootDir, objectPath), object, 0600)
}
