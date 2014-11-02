package minio

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

type Server struct {
}

func (server *Server) Start() error {
	r := mux.NewRouter()
	r.HandleFunc("/", HelloHandler)
	fmt.Println("Running http server on port 8080")
	return http.ListenAndServe(":8080", r)
}

func HelloHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Host: "+req.Host)
}
