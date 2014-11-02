package minio

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

func GatewayHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Gateway")
}

func RegisterGatewayHandlers(router *mux.Router) {
	router.HandleFunc("/gateway/rpc", GatewayHandler)
}
