package storage

import (
	"bytes"
	"errors"
	"io"
)

type Storage struct{}

func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) error {
	// TODO synchronize access
	// get object
	objectData := "OBJECT: " + bucket + " - " + object
	objectBuffer := bytes.NewBufferString(objectData)
	// copy object to writer
	_, err := io.Copy(w, objectBuffer)
	return err
}

func Start() (chan<- string, <-chan error, *Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	errorChannel <- errors.New("STORAGE MSG")
	errorChannel <- errors.New("STORAGE MSG")
	errorChannel <- errors.New("STORAGE MSG")
	close(errorChannel)
}
