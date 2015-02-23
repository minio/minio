package minioapi

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
)

func (server *minioApi) getObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string

	acceptsContentType := getContentType(req)
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	metadata, err := server.storage.GetObjectMetadata(bucket, object)
	switch err := err.(type) {
	case nil: // success
		{
			log.Println("Found: " + bucket + "#" + object)
			writeObjectHeaders(w, metadata)
			if _, err := server.storage.CopyObjectToWriter(w, bucket, object); err != nil {
				log.Println(err)
			}
		}
	case mstorage.ObjectNotFound:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ObjectNameInvalid:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			// Embed errors log on serve side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) headObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	acceptsContentType := getContentType(req)
	vars := mux.Vars(req)
	bucket = vars["bucket"]
	object = vars["object"]

	metadata, err := server.storage.GetObjectMetadata(bucket, object)
	switch err := err.(type) {
	case nil:
		writeObjectHeaders(w, metadata)
	case mstorage.ObjectNotFound:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ObjectNameInvalid:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			// Embed error log on server side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	var object, bucket string
	vars := mux.Vars(req)
	acceptsContentType := getContentType(req)
	bucket = vars["bucket"]
	object = vars["object"]

	resources := getBucketResources(req.URL.Query())
	if resources.policy == true && object == "" {
		server.putBucketPolicyHandler(w, req)
		return
	}

	err := server.storage.StoreObject(bucket, object, "", req.Body)
	switch err := err.(type) {
	case nil:
		w.Header().Set("Server", "Minio")
		w.Header().Set("Connection", "close")
	case mstorage.ImplementationError:
		{
			// Embed error log on server side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ObjectExists:
		{
			error := errorCodeError(NotImplemented)
			errorResponse := getErrorResponse(error, "/"+bucket+"/"+object)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}

}
