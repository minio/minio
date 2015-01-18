package storage

import (
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

func GetHttpHandler() http.Handler {
	mux := mux.NewRouter()
	mux.HandleFunc("/", storageHandler)
	return mux
}

func storageHandler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "MINIO")
}

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
