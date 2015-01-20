package storage

import (
	"bytes"
	"errors"
	"io"
)

type Storage struct {
	data map[string][]byte
}

type GenericError struct {
	bucket string
	path   string
}

type ObjectNotFound GenericError

func (self ObjectNotFound) Error() string {
	return "Not Found: " + self.bucket + "#" + self.path
}

func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// TODO synchronize access
	// get object
	key := bucket + ":" + object
	if val, ok := storage.data[key]; ok {
		objectBuffer := bytes.NewBuffer(val)
		written, err := io.Copy(w, objectBuffer)
		return written, err
	} else {
		return 0, ObjectNotFound{bucket: bucket, path: object}
	}
}

func (storage *Storage) StoreObject(bucket string, object string, data io.Reader) {
	key := bucket + ":" + object
	var bytesBuffer bytes.Buffer
	if _, ok := io.Copy(&bytesBuffer, data); ok == nil {
		storage.data[key] = bytesBuffer.Bytes()
	}
}

func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{
		data: make(map[string][]byte),
	}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	errorChannel <- errors.New("STORAGE MSG")
	errorChannel <- errors.New("STORAGE MSG")
	errorChannel <- errors.New("STORAGE MSG")
	close(errorChannel)
}
