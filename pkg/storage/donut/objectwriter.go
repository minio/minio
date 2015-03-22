package donut

import (
	"errors"
)

type objectWriter struct {
	metadata map[string]string
}

func (obj objectWriter) Write(data []byte) (length int, err error) {
	return 11, nil
}

func (obj objectWriter) Close() error {
	return nil
}

func (obj objectWriter) CloseWithError(err error) error {
	return errors.New("Not Implemented")
}

func (obj objectWriter) SetMetadata(metadata map[string]string) error {
	for k := range obj.metadata {
		delete(obj.metadata, k)
	}
	for k, v := range metadata {
		obj.metadata[k] = v
	}
	return nil
}

func (obj objectWriter) GetMetadata() (map[string]string, error) {
	ret := make(map[string]string)
	for k, v := range obj.metadata {
		ret[k] = v
	}
	return ret, nil
}
