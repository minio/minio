package storage

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

type ObjectStorage interface {
	Get(path string) ([]byte, error)
	Put(path string, object []byte) error
}

func RegisterStorageHandlers(router *mux.Router) {
	router.HandleFunc("/storage/rpc", StorageHandler)
}

func StorageHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Storage")
}
