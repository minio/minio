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

	x "github.com/gorilla/mux"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/config"
)

const (
	dateFormat = "2006-01-02T15:04:05.000Z"
)

type minioApi struct {
	domain  string
	storage mstorage.Storage
}

func pathMux(api minioApi, mux *x.Router) *x.Router {
	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")

	return mux
}

func domainMux(api minioApi, mux *x.Router) *x.Router {
	mux.HandleFunc("/",
		api.listObjectsHandler).Host("{bucket}" + "." + api.domain).Methods("GET")
	mux.HandleFunc("/{object:.*}",
		api.getObjectHandler).Host("{bucket}" + "." + api.domain).Methods("GET")
	mux.HandleFunc("/{object:.*}",
		api.headObjectHandler).Host("{bucket}" + "." + api.domain).Methods("HEAD")
	mux.HandleFunc("/{object:.*}",
		api.putObjectHandler).Host("{bucket}" + "." + api.domain).Methods("PUT")
	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")

	return mux
}

func getMux(api minioApi, mux *x.Router) *x.Router {
	switch true {
	case api.domain == "":
		return pathMux(api, mux)
	case api.domain != "":
		s := mux.Host(api.domain).Subrouter()
		return domainMux(api, s)
	}
	return nil
}

func HttpHandler(domain string, storage mstorage.Storage) http.Handler {
	var mux *x.Router
	var api = minioApi{}
	api.storage = storage
	api.domain = domain

	r := x.NewRouter()
	mux = getMux(api, r)

	var conf = config.Config{}
	if err := conf.SetupConfig(); err != nil {
		log.Fatal(err)
	}

	return validateHandler(conf, ignoreResourcesHandler(mux))
}
