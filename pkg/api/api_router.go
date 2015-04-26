/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

package api

import (
	"log"
	"net/http"

	router "github.com/gorilla/mux"
	"github.com/minio-io/minio/pkg/api/config"
	"github.com/minio-io/minio/pkg/api/quota"
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
	"time"
)

// private use
type minioAPI struct {
	domain string
	driver drivers.Driver
}

// Path based routing
func pathMux(api minioAPI, mux *router.Router) *router.Router {
	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", api.headBucketHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")

	return mux
}

// Domain based routing
func domainMux(api minioAPI, mux *router.Router) *router.Router {
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
	mux.HandleFunc("/{bucket}", api.headBucketHandler).Methods("HEAD")

	return mux
}

// Get proper router based on domain availability
func getMux(api minioAPI, mux *router.Router) *router.Router {
	switch true {
	case api.domain == "":
		return pathMux(api, mux)
	case api.domain != "":
		s := mux.Host(api.domain).Subrouter()
		return domainMux(api, s)
	}
	return nil
}

// HTTPHandler - http wrapper handler
func HTTPHandler(domain string, driver drivers.Driver) http.Handler {
	var mux *router.Router
	var api = minioAPI{}
	api.driver = driver
	api.domain = domain

	r := router.NewRouter()
	mux = getMux(api, r)

	var conf = config.Config{}
	if err := conf.SetupConfig(); err != nil {
		log.Fatal(iodine.New(err, map[string]string{"domain": domain}))
	}

	h := validateHandler(conf, ignoreResourcesHandler(mux))
	h = quota.BandwidthCap(h, 1*1024*1024*1024, time.Duration(30*time.Minute))
	h = quota.BandwidthCap(h, 1024*1024*1024, time.Duration(24*time.Hour))
	h = quota.RequestLimit(h, 100, time.Duration(30*time.Minute))
	h = quota.RequestLimit(h, 1000, time.Duration(24*time.Hour))
	h = quota.ConnectionLimit(h, 5)
	return h
}
