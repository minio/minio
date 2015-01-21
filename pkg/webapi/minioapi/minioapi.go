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
	"time"

	"github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
)

type minioApi struct {
	storage mstorage.Storage
}

type encoder interface {
	Encode(v interface{}) error
}

func HttpHandler(storage mstorage.Storage) http.Handler {
	mux := mux.NewRouter()
	api := minioApi{
		storage: storage,
	}
	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/", api.listObjectsHandler).Methods("GET")
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

func (server *minioApi) listBucketsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	prefix, ok := vars["prefix"]
	if ok == false {
		prefix = ""
	}
	contentType := "xml"
	if req.Header["Accept"][0] == "application/json" {
		contentType = "json"
	}
	buckets := server.storage.ListBuckets(prefix)
	response := generateBucketsListResult(buckets)

	var bytesBuffer bytes.Buffer
	var encoder encoder
	if contentType == "json" {
		w.Header().Set("Content-Type", "application/json")
		encoder = json.NewEncoder(&bytesBuffer)
	} else {
		w.Header().Set("Content-Type", "application/xml")
		encoder = xml.NewEncoder(&bytesBuffer)
	}
	encoder.Encode(response)

	w.Write(bytesBuffer.Bytes())

}

func (server *minioApi) listObjectsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	//delimiter, ok := vars["delimiter"]
	//encodingType, ok := vars["encoding-type"]
	//marker, ok := vars["marker"]
	//maxKeys, ok := vars["max-keys"]
	bucket := vars["bucket"]
	//bucket, ok := vars["bucket"]
	//if ok == false {
	//	w.WriteHeader(http.StatusBadRequest)
	//	return
	//}
	prefix, ok := vars["prefix"]
	if ok == false {
		prefix = ""
	}

	contentType := "xml"

	if req.Header["Accept"][0] == "application/json" {
		contentType = "json"
	}

	objects := server.storage.ListObjects(bucket, prefix, 1000)
	response := generateObjectsListResult(bucket, objects)

	var bytesBuffer bytes.Buffer
	var encoder encoder
	if contentType == "json" {
		w.Header().Set("Content-Type", "application/json")
		encoder = json.NewEncoder(&bytesBuffer)
	} else {
		w.Header().Set("Content-Type", "application/xml")
		encoder = xml.NewEncoder(&bytesBuffer)
	}
	encoder.Encode(response)

	w.Write(bytesBuffer.Bytes())
}

func (server *minioApi) putObjectHandler(w http.ResponseWriter, req *http.Request) {
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
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	err := server.storage.StoreBucket(bucket)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
}

func generateBucketsListResult(buckets []mstorage.BucketMetadata) (data BucketListResponse) {
	listbuckets := []Bucket{}

	owner := Owner{
		ID:          "minio",
		DisplayName: "minio",
	}

	for _, bucket := range buckets {
		listbucket := Bucket{
			Name:         bucket.Name,
			CreationDate: formatDate(bucket.Created),
		}
		listbuckets = append(listbuckets, listbucket)
	}

	data = BucketListResponse{
		Owner:   owner,
		Buckets: listbuckets,
	}
	return
}

func generateObjectsListResult(bucket string, objects []mstorage.ObjectMetadata) (data ObjectListResponse) {
	contents := []Content{}

	owner := Owner{
		ID:          "minio",
		DisplayName: "minio",
	}

	for _, object := range objects {
		content := Content{
			Key:          object.Key,
			LastModified: formatDate(object.SecCreated),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: "STANDARD",
			Owner:        owner,
		}
		contents = append(contents, content)
	}
	data = ObjectListResponse{
		Name:        bucket,
		Contents:    contents,
		MaxKeys:     len(objects),
		IsTruncated: false,
	}
	return
}

func formatDate(sec int64) string {
	timeStamp := time.Unix(sec, 0)
	return timeStamp.Format("2006-01-02T15:04:05.000Z")
}
