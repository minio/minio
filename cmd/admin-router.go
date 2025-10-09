// Copyright (c) 2015-2021 MinIO, Inc.
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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
package cmd

import (
	"net/http"

	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/gzip"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
)

const (
	adminPathPrefix              = minioReservedBucketPath + "/admin"
	adminAPIVersion              = madmin.AdminAPIVersion
	adminAPIVersionPrefix        = SlashSeparator + adminAPIVersion
	adminAPISiteReplicationDevNull = "/site-replication/devnull"
	adminAPISiteReplicationNetPerf = "/site-replication/netperf"
	adminAPIClientDevNull        = "/speedtest/client/devnull"
	adminAPIClientDevExtraTime   = "/speedtest/client/devnull/extratime"
)

var gzipHandler = func() func(http.Handler) http.HandlerFunc {
	gz, err := gzhttp.NewWrapper(gzhttp.MinSize(1000), gzhttp.CompressionLevel(gzip.BestSpeed))
	if err != nil {
		// Static params, so this is very unlikely.
		logger.Fatal(err, "Unable to initialize server")
	}
	return gz
}()

// Set of handler options as bit flags
type hFlag uint8

const (
	// this flag disables gzip compression of responses
	noGZFlag = 1 << iota
	// this flag enables tracing body and headers instead of just headers
	traceAllFlag
	// pass this flag to skip checking if object layer is available
	noObjLayerFlag
)

// Has checks if the given flag is enabled in `h`.
func (h hFlag) Has(flag hFlag) bool {
	// Use bitwise-AND and check if the result is non-zero.
	return h&flag != 0
}

// adminMiddleware performs some common admin handler functionality for all
// handlers:
//
// - updates request context with `logger.ReqInfo` and api name based on the
// name of the function handler passed (this handler must be a method
