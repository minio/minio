package httpserver

import (
	"log"
	"net/http"
)

func Start(handler http.Handler) (chan<- string, <-chan error) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel, handler)
	return ctrlChannel, errorChannel
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, router http.Handler) {
	log.Println("Starting HTTP Server")
	err := http.ListenAndServe(":8080", router)
	errorChannel <- err
}
