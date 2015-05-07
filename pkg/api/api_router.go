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
	"github.com/minio-io/minio/pkg/api/logging"
	"github.com/minio-io/minio/pkg/api/quota"
	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/drivers"
)

// private use
type minioAPI struct {
	driver drivers.Driver
}

// HTTPHandler - http wrapper handler
func HTTPHandler(driver drivers.Driver) http.Handler {
	var mux *router.Router
	var api = minioAPI{}
	api.driver = driver

	mux = router.NewRouter()
	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", api.headBucketHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")

	var conf = config.Config{}
	if err := conf.SetupConfig(); err != nil {
		log.Fatal(iodine.New(err, nil))
	}
	h := timeValidityHandler(mux)
	h = ignoreResourcesHandler(h)
	h = validateRequestHandler(conf, h)
	//	h = quota.BandwidthCap(h, 25*1024*1024, time.Duration(30*time.Minute))
	//	h = quota.BandwidthCap(h, 100*1024*1024, time.Duration(24*time.Hour))
	//	h = quota.RequestLimit(h, 100, time.Duration(30*time.Minute))
	//	h = quota.RequestLimit(h, 1000, time.Duration(24*time.Hour))
	h = quota.ConnectionLimit(h, 5)
	h = logging.LogHandler(h)
	return h
}
