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

func (server *minioApi) listObjectsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]

	resources := getBucketResources(req.URL.Query())
	if resources.policy == true {
		server.getBucketPolicyHandler(w, req)
		return
	}

	acceptsContentType := getContentType(req)
	objects, isTruncated, err := server.storage.ListObjects(bucket, resources.prefix, 1000)
	switch err := err.(type) {
	case nil: // success
		{
			response := generateObjectsListResult(bucket, objects, isTruncated)
			w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
		}
	case mstorage.BucketNotFound:
		{
			error := errorCodeError(NoSuchBucket)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			// Embed error log on server side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ObjectNameInvalid:
		{
			error := errorCodeError(NoSuchKey)
			errorResponse := getErrorResponse(error, resources.prefix)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	acceptsContentType := getContentType(req)
	buckets, err := server.storage.ListBuckets()
	switch err := err.(type) {
	case nil:
		{
			response := generateBucketsListResult(buckets)
			w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "")
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BackendCorrupted:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, "")
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.storage.StoreBucket(bucket)

	resources := getBucketResources(req.URL.Query())
	if resources.policy == true {
		server.putBucketPolicyHandler(w, req)
		return
	}

	acceptsContentType := getContentType(req)
	switch err := err.(type) {
	case nil:
		{
			w.Header().Set("Server", "Minio")
			w.Header().Set("Connection", "close")
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BucketExists:
		{
			error := errorCodeError(BucketAlreadyExists)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			// Embed errors log on serve side
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, bucket)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}
