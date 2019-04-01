/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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

package cmd

import (
	lhttp "github.com/minio/minio/cmd/logger/target/http"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
	config "github.com/minio/minio/pkg/server-config"
)

// Region specific envs.
const (
	EnvRegion = "MINIO_REGION"
)

// Worm specific envs.
const (
	EnvWORM = "MINIO_WORM"
)

// Compression specific envs.
const (
	EnvCompress           = "MINIO_COMPRESS"
	EnvCompressExtensions = "MINIO_COMPRESS_EXTENSIONS"
	EnvCompressMimeTypes  = "MINIO_COMPRESS_MIME_TYPES"
)

// Cache specific envs.
const (
	EnvCacheDrives              = "MINIO_CACHE_DRIVES"
	EnvCacheExclude             = "MINIO_CACHE_EXCLUDE"
	EnvCacheExpiry              = "MINIO_CACHE_EXPIRY"
	EnvCacheQuota               = "MINIO_CACHE_QUOTA"
	EnvCacheEncryptionMasterKey = "MINIO_CACHE_ENCRYPTION_MASTER_KEY"
)

// Storage class specific envs.
const (
	// EnvReducedRedundancyStorageClass - reduced redundancy storage class environment variable
	EnvReducedRedundancyStorageClass = "MINIO_STORAGE_CLASS_RRS"
	// EnvStandardStorageClass - standard storage class environment variable
	EnvStandardStorageClass = "MINIO_STORAGE_CLASS_STANDARD"
)

// Logger specific envs.
const (
	EnvLoggerHTTPEndpoint      = "MINIO_LOGGER_HTTP_ENDPOINT"
	EnvLoggerHTTPAuditEndpoint = "MINIO_LOGGER_HTTP_AUDIT_ENDPOINT"
)

// NewAuditLoggerHTTP - initialize audit logger http
func NewAuditLoggerHTTP(kvs config.KVS) (*lhttp.Target, error) {
	if kvs.Get(config.State) != config.StateEnabled {
		return nil, nil
	}

	loggerEndpoint := env.Get(EnvLoggerHTTPAuditEndpoint, kvs.Get("endpoint"))
	loggerUserAgent := getUserAgent(getMinioMode())

	u, err := xnet.ParseURL(loggerEndpoint)
	if err != nil {
		return nil, err
	}

	// Enable Audit logging.
	return lhttp.New(u.String(), loggerUserAgent, NewCustomHTTPTransport()), nil
}

// NewLoggerHTTP - initialize logger http
func NewLoggerHTTP(kvs config.KVS) (*lhttp.Target, error) {
	loggerEndpoint := env.Get(EnvLoggerHTTPEndpoint, kvs.Get(config.LoggerHTTPEndpoint))
	loggerUserAgent := getUserAgent(getMinioMode())
	if kvs.Get(config.State) != config.StateEnabled {
		return nil, nil
	}
	u, err := xnet.ParseURL(loggerEndpoint)
	if err != nil {
		return nil, err
	}
	return lhttp.New(u.String(), loggerUserAgent, NewCustomHTTPTransport()), nil
}
