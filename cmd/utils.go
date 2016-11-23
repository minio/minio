/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"encoding/json"

	humanize "github.com/dustin/go-humanize"
	"github.com/pkg/profile"
)

// make a copy of http.Header
func cloneHeader(h http.Header) http.Header {
	h2 := make(http.Header, len(h))
	for k, vv := range h {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h2[k] = vv2

	}
	return h2
}

// checkDuplicates - function to validate if there are duplicates in a slice of strings.
func checkDuplicateStrings(list []string) error {
	// Empty lists are not allowed.
	if len(list) == 0 {
		return errInvalidArgument
	}
	// Empty keys are not allowed.
	for _, key := range list {
		if key == "" {
			return errInvalidArgument
		}
	}
	listMaps := make(map[string]int)
	// Navigate through each configs and count the entries.
	for _, key := range list {
		listMaps[key]++
	}
	// Validate if there are any duplicate counts.
	for key, count := range listMaps {
		if count != 1 {
			return fmt.Errorf("Duplicate key: \"%s\" found of count: \"%d\"", key, count)
		}
	}
	// No duplicates.
	return nil
}

// checkDuplicates - function to validate if there are duplicates in a slice of endPoints.
func checkDuplicateEndpoints(endpoints []*url.URL) error {
	var strs []string
	for _, ep := range endpoints {
		strs = append(strs, ep.String())
	}
	return checkDuplicateStrings(strs)
}

// Find local node through the command line arguments. Returns in `host:port` format.
func getLocalAddress(srvCmdConfig serverCmdConfig) string {
	if !globalIsDistXL {
		return srvCmdConfig.serverAddr
	}
	for _, ep := range srvCmdConfig.endpoints {
		// Validates if remote endpoint is local.
		if isLocalStorage(ep) {
			return ep.Host
		}
	}
	return ""
}

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}, size int64) error {
	var lbody io.Reader
	if size > 0 {
		lbody = io.LimitReader(body, size)
	} else {
		lbody = body
	}
	d := xml.NewDecoder(lbody)
	return d.Decode(v)
}

// checkValidMD5 - verify if valid md5, returns md5 in bytes.
func checkValidMD5(md5 string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
}

/// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// maximum object size per PUT request is 5GiB
	maxObjectSize = 5 * humanize.GiByte
	// minimum Part size for multipart upload is 5MiB
	minPartSize = 5 * humanize.MiByte
	// maximum Part ID for multipart upload is 10000 (Acceptable values range from 1 to 10000 inclusive)
	maxPartID = 10000
)

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size int64) bool {
	return size > maxObjectSize
}

// Check if part size is more than or equal to minimum allowed size.
func isMinAllowedPartSize(size int64) bool {
	return size >= minPartSize
}

// isMaxPartNumber - Check if part ID is greater than the maximum allowed ID.
func isMaxPartID(partID int) bool {
	return partID > maxPartID
}

func contains(stringList []string, element string) bool {
	for _, e := range stringList {
		if e == element {
			return true
		}
	}
	return false
}

// Contains endpoint returns true if endpoint found in the list of input endpoints.
func containsEndpoint(endpoints []*url.URL, endpoint *url.URL) bool {
	for _, ep := range endpoints {
		if *ep == *endpoint {
			return true
		}
	}
	return false
}

// urlPathSplit - split url path into bucket and object components.
func urlPathSplit(urlPath string) (bucketName, prefixName string) {
	if urlPath == "" {
		return urlPath, ""
	}
	urlPath = strings.TrimPrefix(urlPath, "/")
	i := strings.Index(urlPath, "/")
	if i != -1 {
		return urlPath[:i], urlPath[i+1:]
	}
	return urlPath, ""
}

// Starts a profiler returns nil if profiler is not enabled, caller needs to handle this.
func startProfiler(profiler string) interface {
	Stop()
} {
	// Enable profiler if ``_MINIO_PROFILER`` is set. Supported options are [cpu, mem, block].
	switch profiler {
	case "cpu":
		return profile.Start(profile.CPUProfile, profile.NoShutdownHook)
	case "mem":
		return profile.Start(profile.MemProfile, profile.NoShutdownHook)
	case "block":
		return profile.Start(profile.BlockProfile, profile.NoShutdownHook)
	default:
		return nil
	}
}

// Global profiler to be used by service go-routine.
var globalProfiler interface {
	Stop()
}

// dump the request into a string in JSON format.
func dumpRequest(r *http.Request) string {
	header := cloneHeader(r.Header)
	header.Set("Host", r.Host)
	req := struct {
		Method string      `json:"method"`
		Path   string      `json:"path"`
		Query  string      `json:"query"`
		Header http.Header `json:"header"`
	}{r.Method, r.URL.Path, r.URL.RawQuery, header}
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return "<error dumping request>"
	}
	return string(jsonBytes)
}
