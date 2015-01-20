package minioapi

import (
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
	server.storage.CopyObjectToWriter(w, bucket, object)
}

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	server.storage.StoreObject(bucket, object, req.Body)
}
