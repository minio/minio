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
	"strings"
	"time"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/config"
	"github.com/minio-io/minio/pkg/utils/crypto/signers"
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

type vHandler struct {
	conf    config.Config
	handler http.Handler
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

// grab AccessKey from authorization header
func stripAccessKey(r *http.Request) string {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) < 2 {
		return ""
	}
	splits := strings.Split(fields[1], ":")
	if len(splits) < 2 {
		return ""
	}
	return splits[0]
}

func validateHandler(conf config.Config, h http.Handler) http.Handler {
	return vHandler{conf, h}
}

func (h vHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	accessKey := stripAccessKey(r)
	if accessKey != "" {
		if err := h.conf.ReadConfig(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			user := h.conf.GetKey(accessKey)
			ok, err := signers.ValidateRequest(user, r)
			if ok {
				h.handler.ServeHTTP(w, r)
			} else {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(err.Error()))
			}
		}
	} else {
		//No access key found, handle this more appropriately
		//TODO: Remove this after adding tests to support signature
		//request
		h.handler.ServeHTTP(w, r)
		//Add this line, to reply back for invalid requests
		//w.WriteHeader(http.StatusUnauthorized)
		//w.Write([]byte("Authorization header malformed")
	}
}

func ignoreUnimplementedResources(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ignoreUnImplementedObjectResources(r) || ignoreUnImplementedBucketResources(r) {
			w.WriteHeader(http.StatusNotImplemented)
		} else {
			h.ServeHTTP(w, r)
		}
	})
}

func (server *minioApi) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	prefix, ok := vars["prefix"]
	if !ok {
		prefix = ""
	}

	acceptsContentType := getContentType(req)
	buckets, err := server.storage.ListBuckets(prefix)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	response := generateBucketsListResult(buckets)
	w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
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
		response := generateObjectsListResult(bucket, objects, isTruncated)
		w.Write(writeObjectHeadersAndResponse(w, response, acceptsContentType))
	case mstorage.BucketNotFound:
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(err.Error()))
	case mstorage.ImplementationError:
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
	}
}

func (server *minioApi) putBucketHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.storage.StoreBucket(bucket)
	switch err := err.(type) {
	case nil:
		w.Header().Set("Server", "Minio")
		w.Header().Set("Connection", "close")
	case mstorage.BucketNameInvalid:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
	case mstorage.BucketExists:
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(err.Error()))
	}
}

func (server *minioApi) getObjectHandler(w http.ResponseWriter, req *http.Request) {
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
			w.Write([]byte(err.Error()))
		}
	default:
		{
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
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

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	err := server.storage.StoreObject(bucket, object, "", req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("Server", "Minio")
	w.Header().Set("Connection", "close")
}

func writeObjectHeadersAndResponse(w http.ResponseWriter, response interface{}, acceptsType contentType) []byte {
	var bytesBuffer bytes.Buffer
	var encoder encoder
	if acceptsType == xmlType {
		w.Header().Set("Content-Type", "application/xml")
		encoder = xml.NewEncoder(&bytesBuffer)
	} else if acceptsType == jsonType {
		w.Header().Set("Content-Type", "application/json")
		encoder = json.NewEncoder(&bytesBuffer)
	}
	w.Header().Set("Server", "Minio")
	w.Header().Set("Connection", "close")
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}

// Write Object Header helper
func writeObjectHeaders(w http.ResponseWriter, metadata mstorage.ObjectMetadata) {
	lastModified := metadata.Created.Format(time.RFC1123)
	w.Header().Set("ETag", metadata.ETag)
	w.Header().Set("Server", "Minio")
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.Header().Set("Content-Type", metadata.ContentType)
	w.Header().Set("Connection", "close")
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

//// helpers

// Checks requests for unimplemented resources
func ignoreUnImplementedBucketResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

func ignoreUnImplementedObjectResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}

func getContentType(req *http.Request) contentType {
	if _, ok := req.Header["Accept"]; ok {
		if req.Header["Accept"][0] == "application/json" {
			return jsonType
		}
	}
	return xmlType
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
