package cmd

import (
	"io/ioutil"
	"os"
	"path"
)

type KVEmulator struct {
	path string
}

func (k *KVEmulator) Put(keyStr string, value []byte) error {
	fullPath := pathJoin(k.path, keyStr)
	os.MkdirAll(path.Dir(fullPath), 0777)
	if err := ioutil.WriteFile(fullPath, value, 0644); err != nil {
		return errFileNotFound
	}
	return nil
}

func (k *KVEmulator) Get(keyStr string) ([]byte, error) {
	b, err := ioutil.ReadFile(pathJoin(k.path, keyStr))
	if err != nil {
		return nil, errFileNotFound
	}
	return b, nil
}

func (k *KVEmulator) Delete(keyStr string) error {
	if err := os.Remove(pathJoin(k.path, keyStr)); err != nil {
		return errFileNotFound
	}
	return nil
}
