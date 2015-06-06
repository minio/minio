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
	"net/http"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/api/logging"
	"github.com/minio/minio/pkg/api/quota"
	"github.com/minio/minio/pkg/storage/drivers"
)

type minioAPI struct {
	driver drivers.Driver
}

// Config api configurable parameters
type Config struct {
	ConnectionLimit int
	driver          drivers.Driver
}

// GetDriver - get a an existing set driver
func (c Config) GetDriver() drivers.Driver {
	return c.driver
}

// SetDriver - set a new driver
func (c *Config) SetDriver(driver drivers.Driver) {
	c.driver = driver
}

// HTTPHandler - http wrapper handler
func HTTPHandler(config Config) http.Handler {
	var mux *router.Router
	var api = minioAPI{}
	api.driver = config.GetDriver()

	mux = router.NewRouter()
	mux.HandleFunc("/", api.listBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.listObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.putBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", api.headBucketHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", api.listObjectPartsHandler).Queries("uploadId", "{uploadId:.*}").Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.completeMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", api.newMultipartUploadHandler).Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", api.abortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("DELETE")
	mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")

	handler := validContentTypeHandler(mux)
	handler = timeValidityHandler(handler)
	handler = ignoreResourcesHandler(handler)
	handler = validateAuthHeaderHandler(handler)
	//	h = quota.BandwidthCap(h, 25*1024*1024, time.Duration(30*time.Minute))
	//	h = quota.BandwidthCap(h, 100*1024*1024, time.Duration(24*time.Hour))
	//	h = quota.RequestLimit(h, 100, time.Duration(30*time.Minute))
	//	h = quota.RequestLimit(h, 1000, time.Duration(24*time.Hour))
	handler = quota.ConnectionLimit(handler, config.ConnectionLimit)
	handler = logging.LogHandler(handler)
	return handler
}
