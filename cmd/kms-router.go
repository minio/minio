// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"net/http"

	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/gzip"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
)

const (
	kmsPathPrefix       = minioReservedBucketPath + "/kms"
	kmsAPIVersion       = "v1"
	kmsAPIVersionPrefix = SlashSeparator + kmsAPIVersion
)

type kmsAPIHandlers struct{}

// registerKMSRouter - Registers KMS APIs
func registerKMSRouter(router *mux.Router) {
	kmsAPI := kmsAPIHandlers{}
	kmsRouter := router.PathPrefix(kmsPathPrefix).Subrouter()

	KMSVersions := []string{
		kmsAPIVersionPrefix,
	}

	gz, err := gzhttp.NewWrapper(gzhttp.MinSize(1000), gzhttp.CompressionLevel(gzip.BestSpeed))
	if err != nil {
		// Static params, so this is very unlikely.
		logger.Fatal(err, "Unable to initialize server")
	}

	for _, version := range KMSVersions {
		// KMS Status APIs
		kmsRouter.Methods(http.MethodGet).Path(version + "/status").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSStatusHandler)))
		kmsRouter.Methods(http.MethodGet).Path(version + "/metrics").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSMetricsHandler)))
		kmsRouter.Methods(http.MethodGet).Path(version + "/apis").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSAPIsHandler)))
		kmsRouter.Methods(http.MethodGet).Path(version + "/version").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSVersionHandler)))
		// KMS Key APIs
		kmsRouter.Methods(http.MethodPost).Path(version+"/key/create").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSCreateKeyHandler))).Queries("key-id", "{key-id:.*}")
		kmsRouter.Methods(http.MethodGet).Path(version+"/key/list").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSListKeysHandler))).Queries("pattern", "{pattern:.*}")
		kmsRouter.Methods(http.MethodGet).Path(version + "/key/status").HandlerFunc(gz(httpTraceAll(kmsAPI.KMSKeyStatusHandler)))
	}

	// If none of the routes match add default error handler routes
	kmsRouter.NotFoundHandler = httpTraceAll(errorResponseHandler)
	kmsRouter.MethodNotAllowedHandler = httpTraceAll(methodNotAllowedHandler("KMS"))
}
