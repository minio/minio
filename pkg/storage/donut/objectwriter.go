package donut

import (
	"errors"
)

type objectWriter struct {
	metadata map[string]string
}

func (self objectWriter) Write(data []byte) (length int, err error) {
	return 11, nil
}

func (self objectWriter) Close() error {
	return nil
}

func (self objectWriter) CloseWithError(err error) error {
	return errors.New("Not Implemented")
}

func (self objectWriter) SetMetadata(metadata map[string]string) error {
	for k := range self.metadata {
		delete(self.metadata, k)
	}
	for k, v := range metadata {
		self.metadata[k] = v
	}
	return nil
}

func (self objectWriter) GetMetadata() (map[string]string, error) {
	ret := make(map[string]string)
	for k, v := range self.metadata {
		ret[k] = v
	}
	return ret, nil
}
