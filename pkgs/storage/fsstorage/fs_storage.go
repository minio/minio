package fsstorage

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type FileSystemStorage struct {
	RootDir string
}

func (storage FileSystemStorage) GetList() ([]byte, error) {
	fileInfos, err := ioutil.ReadDir(storage.RootDir)
	if err != nil {
		return nil, err
	}

	var list []byte
	for _, fi := range fileInfos {
		list = append(list, "{"+fi.Name()+"}\n"...)
	}
	return list, nil
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
