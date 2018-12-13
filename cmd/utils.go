/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/handlers"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/pkg/profile"
)

// IsErrIgnored returns whether given error is ignored or not.
func IsErrIgnored(err error, ignoredErrs ...error) bool {
	return IsErr(err, ignoredErrs...)
}

// IsErr returns whether given error is exact error.
func IsErr(err error, errs ...error) bool {
	for _, exactErr := range errs {
		if err == exactErr {
			return true
		}
	}
	return false
}

// Close Http tracing file.
func stopHTTPTrace() {
	if globalHTTPTraceFile != nil {
		reqInfo := (&logger.ReqInfo{}).AppendTags("traceFile", globalHTTPTraceFile.Name())
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, globalHTTPTraceFile.Close())
		globalHTTPTraceFile = nil
	}
}

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

// Convert url path into bucket and object name.
func urlPath2BucketObjectName(path string) (bucketName, objectName string) {
	// Trim any preceding slash separator.
	urlPath := strings.TrimPrefix(path, slashSeparator)

	// Split urlpath using slash separator into a given number of
	// expected tokens.
	tokens := strings.SplitN(urlPath, slashSeparator, 2)
	bucketName = tokens[0]
	if len(tokens) == 2 {
		objectName = tokens[1]
	}

	// Success.
	return bucketName, objectName
}

// URI scheme constants.
const (
	httpScheme  = "http"
	httpsScheme = "https"
)

// nopCharsetConverter is a dummy charset convert which just copies input to output,
// it is used to ignore custom encoding charset in S3 XML body.
func nopCharsetConverter(label string, input io.Reader) (io.Reader, error) {
	return input, nil
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
	// Ignore any encoding set in the XML body
	d.CharsetReader = nopCharsetConverter
	return d.Decode(v)
}

// checkValidMD5 - verify if valid md5, returns md5 in bytes.
func checkValidMD5(h http.Header) ([]byte, error) {
	md5B64, ok := h["Content-Md5"]
	if ok {
		if md5B64[0] == "" {
			return nil, fmt.Errorf("Content-Md5 header set to empty value")
		}
		return base64.StdEncoding.DecodeString(md5B64[0])
	}
	return []byte{}, nil
}

/// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// Maximum object size per PUT request is 5TB.
	// This is a divergence from S3 limit on purpose to support
	// use cases where users are going to upload large files
	// using 'curl' and presigned URL.
	globalMaxObjectSize = 5 * humanize.TiByte

	// Minimum Part size for multipart upload is 5MiB
	globalMinPartSize = 5 * humanize.MiByte

	// Maximum Part size for multipart upload is 5GiB
	globalMaxPartSize = 5 * humanize.GiByte

	// Maximum Part ID for multipart upload is 10000
	// (Acceptable values range from 1 to 10000 inclusive)
	globalMaxPartID = 10000

	// Default values used while communicating with the cloud backends
	defaultDialTimeout   = 30 * time.Second
	defaultDialKeepAlive = 30 * time.Second
)

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size int64) bool {
	return size > globalMaxObjectSize
}

// // Check if part size is more than maximum allowed size.
func isMaxAllowedPartSize(size int64) bool {
	return size > globalMaxPartSize
}

// Check if part size is more than or equal to minimum allowed size.
func isMinAllowedPartSize(size int64) bool {
	return size >= globalMinPartSize
}

// isMaxPartNumber - Check if part ID is greater than the maximum allowed ID.
func isMaxPartID(partID int) bool {
	return partID > globalMaxPartID
}

func contains(slice interface{}, elem interface{}) bool {
	v := reflect.ValueOf(slice)
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			if v.Index(i).Interface() == elem {
				return true
			}
		}
	}
	return false
}

// profilerWrapper is created becauses pkg/profiler doesn't
// provide any API to calculate the profiler file path in the
// disk since the name of this latter is randomly generated.
type profilerWrapper struct {
	stopFn func()
	pathFn func() string
}

func (p profilerWrapper) Stop() {
	p.stopFn()
}

func (p profilerWrapper) Path() string {
	return p.pathFn()
}

// Starts a profiler returns nil if profiler is not enabled, caller needs to handle this.
func startProfiler(profilerType, dirPath string) (interface {
	Stop()
	Path() string
}, error) {

	var err error
	if dirPath == "" {
		dirPath, err = ioutil.TempDir("", "profile")
		if err != nil {
			return nil, err
		}
	}

	var profiler interface {
		Stop()
	}

	var profilerFileName string

	// Enable profiler and set the name of the file that pkg/pprof
	// library creates to store profiling data.
	switch profilerType {
	case "cpu":
		profiler = profile.Start(profile.CPUProfile, profile.NoShutdownHook, profile.ProfilePath(dirPath))
		profilerFileName = "cpu.pprof"
	case "mem":
		profiler = profile.Start(profile.MemProfile, profile.NoShutdownHook, profile.ProfilePath(dirPath))
		profilerFileName = "mem.pprof"
	case "block":
		profiler = profile.Start(profile.BlockProfile, profile.NoShutdownHook, profile.ProfilePath(dirPath))
		profilerFileName = "block.pprof"
	case "mutex":
		profiler = profile.Start(profile.MutexProfile, profile.NoShutdownHook, profile.ProfilePath(dirPath))
		profilerFileName = "mutex.pprof"
	case "trace":
		profiler = profile.Start(profile.TraceProfile, profile.NoShutdownHook, profile.ProfilePath(dirPath))
		profilerFileName = "trace.out"
	default:
		return nil, errors.New("profiler type unknown")
	}

	return &profilerWrapper{
		stopFn: profiler.Stop,
		pathFn: func() string {
			return filepath.Join(dirPath, profilerFileName)
		},
	}, nil
}

// Global profiler to be used by service go-routine.
var globalProfiler interface {
	// Stop the profiler
	Stop()
	// Return the path of the profiling file
	Path() string
}

// dump the request into a string in JSON format.
func dumpRequest(r *http.Request) string {
	header := cloneHeader(r.Header)
	header.Set("Host", r.Host)
	// Replace all '%' to '%%' so that printer format parser
	// to ignore URL encoded values.
	rawURI := strings.Replace(r.RequestURI, "%", "%%", -1)
	req := struct {
		Method     string      `json:"method"`
		RequestURI string      `json:"reqURI"`
		Header     http.Header `json:"header"`
	}{r.Method, rawURI, header}

	var buffer bytes.Buffer
	enc := json.NewEncoder(&buffer)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(&req); err != nil {
		// Upon error just return Go-syntax representation of the value
		return fmt.Sprintf("%#v", req)
	}

	// Formatted string.
	return strings.TrimSpace(string(buffer.Bytes()))
}

// isFile - returns whether given path is a file or not.
func isFile(path string) bool {
	if fi, err := os.Stat(path); err == nil {
		return fi.Mode().IsRegular()
	}

	return false
}

// UTCNow - returns current UTC time.
func UTCNow() time.Time {
	return time.Now().UTC()
}

// GenETag - generate UUID based ETag
func GenETag() string {
	return ToS3ETag(getMD5Hash([]byte(mustGetUUID())))
}

// ToS3ETag - return checksum to ETag
func ToS3ETag(etag string) string {
	etag = canonicalizeETag(etag)

	if !strings.HasSuffix(etag, "-1") {
		// Tools like s3cmd uses ETag as checksum of data to validate.
		// Append "-1" to indicate ETag is not a checksum.
		etag += "-1"
	}

	return etag
}

// NewCustomHTTPTransport returns a new http configuration
// used while communicating with the cloud backends.
// This sets the value for MaxIdleConnsPerHost from 2 (go default)
// to 100.
func NewCustomHTTPTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   defaultDialTimeout,
			KeepAlive: defaultDialKeepAlive,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{RootCAs: globalRootCAs},
		DisableCompression:    true,
	}
}

// Load the json (typically from disk file).
func jsonLoad(r io.ReadSeeker, data interface{}) error {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return json.NewDecoder(r).Decode(data)
}

// Save to disk file in json format.
func jsonSave(f interface {
	io.WriteSeeker
	Truncate(int64) error
}, data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err = f.Truncate(0); err != nil {
		return err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = f.Write(b)
	if err != nil {
		return err
	}
	return nil
}

// ceilFrac takes a numerator and denominator representing a fraction
// and returns its ceiling. If denominator is 0, it returns 0 instead
// of crashing.
func ceilFrac(numerator, denominator int64) (ceil int64) {
	if denominator == 0 {
		// do nothing on invalid input
		return
	}
	// Make denominator positive
	if denominator < 0 {
		numerator = -numerator
		denominator = -denominator
	}
	ceil = numerator / denominator
	if numerator > 0 && numerator%denominator != 0 {
		ceil++
	}
	return
}

// Returns context with ReqInfo details set in the context.
func newContext(r *http.Request, w http.ResponseWriter, api string) context.Context {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]
	prefix := vars["prefix"]

	if prefix != "" {
		object = prefix
	}
	reqInfo := &logger.ReqInfo{
		DeploymentID: w.Header().Get(responseDeploymentIDKey),
		RequestID:    w.Header().Get(responseRequestIDKey),
		RemoteHost:   handlers.GetSourceIP(r),
		UserAgent:    r.UserAgent(),
		API:          api,
		BucketName:   bucket,
		ObjectName:   object,
	}
	return logger.SetReqInfo(context.Background(), reqInfo)
}

// isNetworkOrHostDown - if there was a network error or if the host is down.
func isNetworkOrHostDown(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *net.DNSError, *net.OpError, net.UnknownNetworkError:
		return true
	case *url.Error:
		// For a URL error, where it replies back "connection closed"
		if strings.Contains(err.Error(), "Connection closed by foreign host") {
			return true
		}
		return true
	default:
		if strings.Contains(err.Error(), "net/http: TLS handshake timeout") {
			// If error is - tlsHandshakeTimeoutError,.
			return true
		} else if strings.Contains(err.Error(), "i/o timeout") {
			// If error is - tcp timeoutError.
			return true
		} else if strings.Contains(err.Error(), "connection timed out") {
			// If err is a net.Dial timeout.
			return true
		} else if strings.Contains(err.Error(), "net/http: HTTP/1.x transport connection broken") {
			return true
		}
	}
	return false
}

var b512pool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 512)
		return &buf
	},
}

// CloseResponse close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
func CloseResponse(respBody io.ReadCloser) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if respBody != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		bufp := b512pool.Get().(*[]byte)
		defer b512pool.Put(bufp)
		io.CopyBuffer(ioutil.Discard, respBody, *bufp)
		respBody.Close()
	}
}

// Used for registering with rest handlers (have a look at registerStorageRESTHandlers for usage example)
// If it is passed ["aaaa", "bbbb"], it returns ["aaaa", "{aaaa:.*}", "bbbb", "{bbbb:.*}"]
func restQueries(keys ...string) []string {
	var accumulator []string
	for _, key := range keys {
		accumulator = append(accumulator, key, "{"+key+":.*}")
	}
	return accumulator
}
