/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minioapi

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/config"
)

const (
	dateFormat = "2006-01-02T15:04:05.000Z"
)

type minioApi struct {
	storage mstorage.Storage
}

// No encoder interface exists, so we create one.
type encoder interface {
	Encode(v interface{}) error
}

func HttpHandler(storage mstorage.Storage) http.Handler {
	mux := mux.NewRouter()
	var api = minioApi{}
	api.storage = storage

	var conf = config.Config{}
	if err := conf.SetupConfig(); err != nil {
		log.Fatal(err)
	}

	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")

	return validateHandler(conf, ignoreUnimplementedResources(mux))
}

func (server *minioApi) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	prefix, ok := vars["prefix"]
	if !ok {
		prefix = ""
	}

	acceptsContentType := getContentType(req)
	buckets, err := server.storage.ListBuckets(prefix)
	switch err := err.(type) {
	case nil:
		{
			response := generateBucketsListResult(buckets)
			w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
		}
	case mstorage.BucketNameInvalid:
		{
			error := errorCodeError(InvalidBucketName)
			errorResponse := getErrorResponse(error, prefix)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.ImplementationError:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, prefix)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	case mstorage.BackendCorrupted:
		{
			log.Println(err)
			error := errorCodeError(InternalError)
			errorResponse := getErrorResponse(error, prefix)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) listObjectsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	prefix, ok := vars["prefix"]
	if !ok {
		prefix = ""
	}

	acceptsContentType := getContentType(req)

	objects, isTruncated, err := server.storage.ListObjects(bucket, prefix, 1000)
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
			errorResponse := getErrorResponse(error, prefix)
			w.WriteHeader(error.HttpStatusCode)
			w.Write(writeErrorResponse(w, errorResponse, acceptsContentType))
		}
	}
}

func (server *minioApi) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.storage.StoreBucket(bucket)

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

func (server *minioApi) getObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	acceptsContentType := getContentType(req)

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
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	acceptsContentType := getContentType(req)

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
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	acceptsContentType := getContentType(req)

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

func generateBucketsListResult(buckets []mstorage.BucketMetadata) BucketListResponse {
	var listbuckets []*Bucket
	var data = BucketListResponse{}
	var owner = Owner{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, bucket := range buckets {
		var listbucket = &Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.Format(dateFormat)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Bucket = listbuckets

	return data
}

// takes a set of objects and prepares the objects for serialization
// input:
// bucket name
// array of object metadata
// results truncated flag
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateObjectsListResult(bucket string, objects []mstorage.ObjectMetadata, isTruncated bool) ObjectListResponse {
	var contents []*Item
	var owner = Owner{}
	var data = ObjectListResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range objects {
		var content = &Item{}
		content.Key = object.Key
		content.LastModified = object.Created.Format(dateFormat)
		content.ETag = object.ETag
		content.Size = object.Size
		content.StorageClass = "STANDARD"
		content.Owner = owner
		contents = append(contents, content)
	}
	data.Name = bucket
	data.Contents = contents
	data.MaxKeys = MAX_OBJECT_LIST
	data.IsTruncated = isTruncated
	return data
}
