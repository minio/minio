package minioapi

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/policy"
)

func (server *minioApi) putBucketPolicyHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	acceptsContentType := getContentType(req)

	policy, ok := policy.Parsepolicy(req.Body)
	if ok == false {
		error := errorCodeError(InvalidPolicyDocument)
		errorResponse := getErrorResponse(error, bucket)
		w.WriteHeader(error.HttpStatusCode)
		w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		return
	}

	err := server.storage.StoreBucketPolicy(bucket, policy)
	switch err := err.(type) {
	case nil:
		{
			w.WriteHeader(http.StatusNoContent)
			writeCommonHeaders(w, getContentString(acceptsContentType))
			w.Header().Set("Connection", "keep-alive")
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BackendCorrupted:
	case mstorage.ImplementationError:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) getBucketPolicyHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	acceptsContentType := getContentType(req)

	p, err := server.storage.GetBucketPolicy(bucket)
	switch err := err.(type) {
	case nil:
		{
			responsePolicy, ret := json.Marshal(p)
			if ret != nil {
				error := errorCodeError(InternalError)
				errorResponse := getErrorResponse(error, bucket)
				w.WriteHeader(error.HttpStatusCode)
				w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
			}
			writeCommonHeaders(w, getContentString(acceptsContentType))
			w.Header().Set("Connection", "keep-alive")
			w.Write(responsePolicy)
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketPolicyNotFound:
		{
			error := errorCodeError(NoSuchBucketPolicy)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BackendCorrupted:
	case mstorage.ImplementationError:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}
