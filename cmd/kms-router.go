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

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/gzip"
	"github.com/minio/minio/internal/logger"
)

const (
	kmsPathPrefix       = minioReservedBucketPath + "/kms"
	kmsAPIVersion       = "v1"
	kmsAPIVersionPrefix = SlashSeparator + kmsAPIVersion
)

// KMSAPIHandlers provides HTTP handlers for MinIO KMS API.
type KMSAPIHandlers struct{}

// registerKMSRouter - Registers KMS APIs
func registerKMSRouter(router *mux.Router) {
	KMSAPI := KMSAPIHandlers{}
	KMSRouter := router.PathPrefix(kmsPathPrefix).Subrouter()

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
		KMSRouter.Methods(http.MethodPost).Path(version + "/status").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSStatusHandler)))
		// KMS Key APIs
		KMSRouter.Methods(http.MethodPost).Path(version+"/key/create").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSCreateKeyHandler))).Queries("key-id", "{key-id:.*}")
		KMSRouter.Methods(http.MethodPost).Path(version+"/key/import").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSImportKeyHandler))).Queries("key-id", "{key-id:.*}")
		KMSRouter.Methods(http.MethodDelete).Path(version+"/key/delete").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSDeleteKeyHandler))).Queries("key-id", "{key-id:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version+"/key/list").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSListKeysHandler))).Queries("pattern", "{pattern:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version + "/key/status").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSKeyStatusHandler)))

		// KMS Policy APIs
		KMSRouter.Methods(http.MethodPost).Path(version+"/policy/set").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSSetPolicyHandler))).Queries("policy", "{policy:.*}")
		KMSRouter.Methods(http.MethodPost).Path(version+"/policy/assign").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSAssignPolicyHandler))).Queries("policy", "{policy:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version+"/policy/describe").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSDescribePolicyHandler))).Queries("policy", "{policy:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version+"/policy/get").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSGetPolicyHandler))).Queries("policy", "{policy:.*}")
		KMSRouter.Methods(http.MethodDelete).Path(version+"/policy/delete").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSDeletePolicyHandler))).Queries("policy", "{policy:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version+"/policy/list").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSListPoliciesHandler))).Queries("pattern", "{pattern:.*}")

		// KMS Identity APIs
		KMSRouter.Methods(http.MethodGet).Path(version+"/identity/describe").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSDescribeIdentityHandler))).Queries("identity", "{identity:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version + "/identity/describe-self").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSDescribeSelfIdentityHandler)))
		KMSRouter.Methods(http.MethodDelete).Path(version+"/identity/delete").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSDeleteIdentityHandler))).Queries("identity", "{identity:.*}")
		KMSRouter.Methods(http.MethodGet).Path(version+"/identity/list").HandlerFunc(gz(httpTraceAll(KMSAPI.KMSListIdentitiesHandler))).Queries("pattern", "{pattern:.*}")
	}

	// If none of the routes match add default error handler routes
	KMSRouter.NotFoundHandler = httpTraceAll(errorResponseHandler)
	KMSRouter.MethodNotAllowedHandler = httpTraceAll(methodNotAllowedHandler("KMS"))
}
