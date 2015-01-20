package minioapi

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
)

type minioApi struct {
	storage *mstorage.Storage
}

func HttpHandler(storage *mstorage.Storage) http.Handler {
	mux := mux.NewRouter()
	api := minioApi{
		storage: storage,
	}
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")
	return mux
}

func (server *minioApi) getObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	_, err := server.storage.CopyObjectToWriter(w, bucket, object)
	switch err := err.(type) {
	case nil: // success
		{
			log.Println("Found: " + bucket + "#" + object)
		}
	case mstorage.ObjectNotFound:
		{
			log.Println(err)
			w.WriteHeader(http.StatusNotFound)
		}
	default:
		{
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	server.storage.StoreObject(bucket, object, req.Body)
}
