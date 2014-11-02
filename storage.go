package minio

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

func RegisterStorageHandlers(router *mux.Router) {
	router.HandleFunc("/storage/rpc", StorageHandler)
}

func StorageHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Storage")
}
