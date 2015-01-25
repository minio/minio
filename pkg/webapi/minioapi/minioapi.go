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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
)

type contentType int

const (
	xmlType contentType = iota
	jsonType
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

	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")

	return mux
}

func (server *minioApi) ignoreUnImplementedBucketResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

func (server *minioApi) ignoreUnImplementedObjectResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}

func (server *minioApi) getObjectHandler(w http.ResponseWriter, req *http.Request) {
	if server.ignoreUnImplementedObjectResources(req) {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

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

func (server *minioApi) headObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]

	metadata, err := server.storage.GetObjectMetadata(bucket, object)
	switch err := err.(type) {
	case nil:
		writeObjectHeaders(w, metadata)
	case mstorage.ObjectNotFound:
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
	default:
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (server *minioApi) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	if server.ignoreUnImplementedBucketResources(req) {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	vars := mux.Vars(req)
	prefix, ok := vars["prefix"]
	if !ok {
		prefix = ""
	}

	contentType := xmlType
	if _, ok := req.Header["Accept"]; ok {
		if req.Header["Accept"][0] == "application/json" {
			contentType = jsonType
		}
	}
	buckets := server.storage.ListBuckets(prefix)
	response := generateBucketsListResult(buckets)

	var bytesBuffer bytes.Buffer
	var encoder encoder
	if contentType == xmlType {
		w.Header().Set("Content-Type", "application/xml")
		encoder = xml.NewEncoder(&bytesBuffer)
	} else if contentType == jsonType {
		w.Header().Set("Content-Type", "application/json")
		encoder = json.NewEncoder(&bytesBuffer)
	}
	encoder.Encode(response)
	w.Write(bytesBuffer.Bytes())
}

func (server *minioApi) listObjectsHandler(w http.ResponseWriter, req *http.Request) {
	if server.ignoreUnImplementedBucketResources(req) {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	prefix, ok := vars["prefix"]
	if !ok {
		prefix = ""
	}

	contentType := xmlType
	if _, ok := req.Header["Accept"]; ok {
		if req.Header["Accept"][0] == "application/json" {
			contentType = jsonType
		}
	}

	objects := server.storage.ListObjects(bucket, prefix, 1000)
	response := generateObjectsListResult(bucket, objects)

	var bytesBuffer bytes.Buffer
	var encoder encoder
	if contentType == xmlType {
		w.Header().Set("Content-Type", "application/xml")
		encoder = xml.NewEncoder(&bytesBuffer)
	} else if contentType == jsonType {
		w.Header().Set("Content-Type", "application/json")
		encoder = json.NewEncoder(&bytesBuffer)
	}

	encoder.Encode(response)
	w.Write(bytesBuffer.Bytes())
}

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	if server.ignoreUnImplementedBucketResources(req) {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	err := server.storage.StoreObject(bucket, object, req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
}

func (server *minioApi) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	if server.ignoreUnImplementedBucketResources(req) {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.storage.StoreBucket(bucket)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
}

// Write Object Header helper
func writeObjectHeaders(w http.ResponseWriter, metadata mstorage.ObjectMetadata) {
	lastModified := metadata.Created.Format(time.RFC1123)
	w.Header().Set("ETag", metadata.ETag)
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Content-Length", strconv.Itoa(metadata.Size))
	w.Header().Set("Content-Type", "text/plain")
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

func generateObjectsListResult(bucket string, objects []mstorage.ObjectMetadata) ObjectListResponse {
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
	data.IsTruncated = false
	return data
}
