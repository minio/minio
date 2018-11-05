// Copyright 2017 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !go1.7

package storage

import (
	"net/http"
)

func withContext(r *http.Request, _ interface{}) *http.Request {
	// In Go 1.6 and below, ignore the context.
	return r
}

// Go 1.6 doesn't have http.Response.Uncompressed, so we can't know whether the Go
// HTTP stack uncompressed a gzip file. As a good approximation, assume that
// the lack of a Content-Length header means that it did uncompress.
func goHTTPUncompressed(res *http.Response) bool {
	return res.Header.Get("Content-Length") == ""
}
