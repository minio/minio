package storage

import "errors"

func Start() (chan<- string, <-chan error) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
	errorChannel <- errors.New("STORAGE MSG")
	errorChannel <- errors.New("STORAGE MSG")
	errorChannel <- errors.New("STORAGE MSG")
	close(errorChannel)
}
