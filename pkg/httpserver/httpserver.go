package httpserver

import (
	"log"
	"net/http"
)

func Start(handler http.Handler, address string) (chan<- string, <-chan error) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel, handler, address)
	return ctrlChannel, errorChannel
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, router http.Handler, address string) {
	log.Println("Starting HTTP Server on " + address)
	err := http.ListenAndServe(address, router)
	errorChannel <- err
	close(errorChannel)
}
