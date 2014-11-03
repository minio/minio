package minio

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

func GatewayHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Gateway")
}

func GatewayGetObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	fmt.Fprintf(w, "bucket: "+bucket)
	fmt.Fprintf(w, "\r")
	fmt.Fprintf(w, "object: "+object)
}

func RegisterGatewayHandlers(router *mux.Router) {
	router.HandleFunc("/{bucket}/{object:.*}", GatewayGetObjectHandler).Methods("GET")
}
