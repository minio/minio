package minioapi

import (
	"bytes"
	"encoding/xml"
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
	mux.HandleFunc("/", api.listHandler).Methods("GET")
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

func (server *minioApi) listHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	//delimiter, ok := vars["delimiter"]
	//encodingType, ok := vars["encoding-type"]
	//marker, ok := vars["marker"]
	//maxKeys, ok := vars["max-keys"]
	bucket := "bucket"
	//bucket, ok := vars["bucket"]
	//if ok == false {
	//	w.WriteHeader(http.StatusBadRequest)
	//	return
	//}
	prefix, ok := vars["prefix"]
	if ok == false {
		prefix = ""
	}

	objects := server.storage.ListObjects(bucket, prefix, 1000)
	response := generateListResult(objects)

	var bytesBuffer bytes.Buffer
	xmlEncoder := xml.NewEncoder(&bytesBuffer)
	xmlEncoder.Encode(response)

	w.Header().Set("Content-Type", "application/xml")
	w.Write(bytesBuffer.Bytes())
}

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	server.storage.StoreObject(bucket, object, req.Body)
}

func generateListResult(objects []mstorage.ObjectMetadata) ListResponse {
	owner := Owner{
		ID:          "MyID",
		DisplayName: "MyDisplayName",
	}
	contents := []Content{
		Content{
			Key:          "one",
			LastModified: "two",
			ETag:         "\"ETag\"",
			Size:         1,
			StorageClass: "three",
			Owner:        owner,
		},
		Content{
			Key:          "four",
			LastModified: "five",
			ETag:         "\"ETag\"",
			Size:         1,
			StorageClass: "six",
			Owner:        owner,
		},
	}
	data := &ListResponse{
		Name:     "name",
		Contents: contents,
	}
	return *data
}
