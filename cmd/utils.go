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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/dustin/go-humanize"
	"github.com/felixge/fgprof"
	"github.com/minio/madmin-go/v3"
	xaudit "github.com/minio/madmin-go/v3/logger/audit"
	"github.com/minio/minio-go/v7"
	miniogopolicy "github.com/minio/minio-go/v7/pkg/policy"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/api"
	xtls "github.com/minio/minio/internal/config/identity/tls"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	ioutilx "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/message/audit"
	"github.com/minio/minio/internal/rest"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/certs"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
	"golang.org/x/oauth2"
)

const (
	slashSeparator = "/"
)

// BucketAccessPolicy - Collection of canned bucket policy at a given prefix.
type BucketAccessPolicy struct {
	Bucket string                     `json:"bucket"`
	Prefix string                     `json:"prefix"`
	Policy miniogopolicy.BucketPolicy `json:"policy"`
}

// IsErrIgnored returns whether given error is ignored or not.
func IsErrIgnored(err error, ignoredErrs ...error) bool {
	return IsErr(err, ignoredErrs...)
}

// IsErr returns whether given error is exact error.
func IsErr(err error, errs ...error) bool {
	for _, exactErr := range errs {
		if errors.Is(err, exactErr) {
			return true
		}
	}
	return false
}

// ErrorRespToObjectError converts MinIO errors to minio object layer errors.
func ErrorRespToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	versionID := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) >= 2 {
		object = params[1]
	}
	if len(params) >= 3 {
		versionID = params[2]
	}

	if xnet.IsNetworkOrHostDown(err, false) {
		return BackendDown{Err: err.Error()}
	}

	minioErr, ok := err.(minio.ErrorResponse)
	if !ok {
		// We don't interpret non MinIO errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	switch minioErr.Code {
	case "SlowDownWrite":
		err = InsufficientWriteQuorum{Bucket: bucket, Object: object}
	case "SlowDownRead":
		err = InsufficientReadQuorum{Bucket: bucket, Object: object}
	case "PreconditionFailed":
		err = PreConditionFailed{}
	case "InvalidRange":
		err = InvalidRange{}
	case "BucketAlreadyOwnedByYou":
		err = BucketAlreadyOwnedByYou{}
	case "BucketNotEmpty":
		err = BucketNotEmpty{}
	case "NoSuchBucketPolicy":
		err = BucketPolicyNotFound{}
	case "NoSuchLifecycleConfiguration":
		err = BucketLifecycleNotFound{}
	case "InvalidBucketName":
		err = BucketNameInvalid{Bucket: bucket}
	case "InvalidPart":
		err = InvalidPart{}
	case "NoSuchBucket":
		err = BucketNotFound{Bucket: bucket}
	case "NoSuchKey":
		if object != "" {
			err = ObjectNotFound{Bucket: bucket, Object: object}
		} else {
			err = BucketNotFound{Bucket: bucket}
		}
	case "NoSuchVersion":
		if object != "" {
			err = ObjectNotFound{Bucket: bucket, Object: object, VersionID: versionID}
		} else {
			err = BucketNotFound{Bucket: bucket}
		}
	case "XMinioInvalidObjectName":
		err = ObjectNameInvalid{}
	case "AccessDenied":
		err = PrefixAccessDenied{
			Bucket: bucket,
			Object: object,
		}
	case "XAmzContentSHA256Mismatch":
		err = hash.SHA256Mismatch{}
	case "NoSuchUpload":
		err = InvalidUploadID{}
	case "EntityTooSmall":
		err = PartTooSmall{}
	case "ReplicationPermissionCheck":
		err = ReplicationPermissionCheck{}
	}

	if minioErr.StatusCode == http.StatusMethodNotAllowed {
		err = toObjectErr(errMethodNotAllowed, bucket, object)
	}
	return err
}

// returns 'true' if either string has space in the
// - beginning of a string
// OR
// - end of a string
func hasSpaceBE(s string) bool {
	return strings.TrimSpace(s) != s
}

func request2BucketObjectName(r *http.Request) (bucketName, objectName string) {
	path, err := getResource(r.URL.Path, r.Host, globalDomainNames)
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}

	return path2BucketObject(path)
}

// path2BucketObjectWithBasePath returns bucket and prefix, if any,
// of a 'path'. basePath is trimmed from the front of the 'path'.
func path2BucketObjectWithBasePath(basePath, path string) (bucket, prefix string) {
	path = strings.TrimPrefix(path, basePath)
	path = strings.TrimPrefix(path, SlashSeparator)
	m := strings.Index(path, SlashSeparator)
	if m < 0 {
		return path, ""
	}
	return path[:m], path[m+len(SlashSeparator):]
}

func path2BucketObject(s string) (bucket, prefix string) {
	return path2BucketObjectWithBasePath("", s)
}

// cloneMSS will clone a map[string]string.
// If input is nil an empty map is returned, not nil.
func cloneMSS(v map[string]string) map[string]string {
	r := make(map[string]string, len(v))
	maps.Copy(r, v)
	return r
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
func xmlDecoder(body io.Reader, v any, size int64) error {
	var lbody io.Reader
	if size > 0 {
		lbody = io.LimitReader(body, size)
	} else {
		lbody = body
	}
	d := xml.NewDecoder(lbody)
	// Ignore any encoding set in the XML body
	d.CharsetReader = nopCharsetConverter
	err := d.Decode(v)
	if errors.Is(err, io.EOF) {
		err = &xml.SyntaxError{
			Line: 0,
			Msg:  err.Error(),
		}
	}
	return err
}

// validateLengthAndChecksum returns if a content checksum is set,
// and will replace r.Body with a reader that checks the provided checksum
func validateLengthAndChecksum(r *http.Request) bool {
	if mdFive := r.Header.Get(xhttp.ContentMD5); mdFive != "" {
		want, err := base64.StdEncoding.DecodeString(mdFive)
		if err != nil {
			return false
		}
		r.Body = hash.NewChecker(r.Body, md5.New(), want, r.ContentLength)
		return true
	}
	cs, err := hash.GetContentChecksum(r.Header)
	if err != nil {
		return false
	}
	if cs == nil || !cs.Type.IsSet() {
		return false
	}
	if cs.Valid() && !cs.Type.Trailing() {
		r.Body = hash.NewChecker(r.Body, cs.Type.Hasher(), cs.Raw, r.ContentLength)
	}
	return true
}

// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// Maximum object size per PUT request is 5TB.
	// This is a divergence from S3 limit on purpose to support
	// use cases where users are going to upload large files
	// using 'curl' and presigned URL.
	globalMaxObjectSize = 5 * humanize.TiByte

	// Minimum Part size for multipart upload is 5MiB
	globalMinPartSize = 5 * humanize.MiByte

	// Maximum Part ID for multipart upload is 10000
	// (Acceptable values range from 1 to 10000 inclusive)
	globalMaxPartID = 10000
)

// isMaxObjectSize - verify if max object size
func isMaxObjectSize(size int64) bool {
	return size > globalMaxObjectSize
}

// Check if part size is more than or equal to minimum allowed size.
func isMinAllowedPartSize(size int64) bool {
	return size >= globalMinPartSize
}

// isMaxPartNumber - Check if part ID is greater than the maximum allowed ID.
func isMaxPartID(partID int) bool {
	return partID > globalMaxPartID
}

// profilerWrapper is created because pkg/profiler doesn't
// provide any API to calculate the profiler file path in the
// disk since the name of this latter is randomly generated.
type profilerWrapper struct {
	// Profile recorded at start of benchmark.
	records map[string][]byte
	stopFn  func() ([]byte, error)
	ext     string
}

// record will record the profile and store it as the base.
func (p *profilerWrapper) record(profileType string, debug int, recordName string) {
	var buf bytes.Buffer
	if p.records == nil {
		p.records = make(map[string][]byte)
	}
	err := pprof.Lookup(profileType).WriteTo(&buf, debug)
	if err != nil {
		return
	}
	p.records[recordName] = buf.Bytes()
}

// Records returns the recorded profiling if any.
func (p profilerWrapper) Records() map[string][]byte {
	return p.records
}

// Stop the currently running benchmark.
func (p profilerWrapper) Stop() ([]byte, error) {
	return p.stopFn()
}

// Extension returns the extension without dot prefix.
func (p profilerWrapper) Extension() string {
	return p.ext
}

// Returns current profile data, returns error if there is no active
// profiling in progress. Stops an active profile.
func getProfileData() (map[string][]byte, error) {
	globalProfilerMu.Lock()
	defer globalProfilerMu.Unlock()

	if len(globalProfiler) == 0 {
		return nil, errors.New("profiler not enabled")
	}

	dst := make(map[string][]byte, len(globalProfiler))
	for typ, prof := range globalProfiler {
		// Stop the profiler
		var err error
		buf, err := prof.Stop()
		delete(globalProfiler, typ)
		if err == nil {
			dst[typ+"."+prof.Extension()] = buf
		}
		for name, buf := range prof.Records() {
			if len(buf) > 0 {
				dst[typ+"-"+name+"."+prof.Extension()] = buf
			}
		}
	}
	return dst, nil
}

func setDefaultProfilerRates() {
	runtime.MemProfileRate = 128 << 10 // 512KB -> 128K - Must be constant throughout application lifetime.
	runtime.SetMutexProfileFraction(0) // Disable until needed
	runtime.SetBlockProfileRate(0)     // Disable until needed
}

// Starts a profiler returns nil if profiler is not enabled, caller needs to handle this.
func startProfiler(profilerType string) (minioProfiler, error) {
	var prof profilerWrapper
	prof.ext = "pprof"
	// Enable profiler and set the name of the file that pkg/pprof
	// library creates to store profiling data.
	switch madmin.ProfilerType(profilerType) {
	case madmin.ProfilerCPU:
		dirPath, err := os.MkdirTemp("", "profile")
		if err != nil {
			return nil, err
		}
		fn := filepath.Join(dirPath, "cpu.out")
		f, err := Create(fn)
		if err != nil {
			return nil, err
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			return nil, err
		}
		prof.stopFn = func() ([]byte, error) {
			pprof.StopCPUProfile()
			err := f.Close()
			if err != nil {
				return nil, err
			}
			defer RemoveAll(dirPath)
			return ioutilx.ReadFile(fn)
		}
	case madmin.ProfilerCPUIO:
		// at 10k or more goroutines fgprof is likely to become
		// unable to maintain its sampling rate and to significantly
		// degrade the performance of your application
		// https://github.com/felixge/fgprof#fgprof
		if n := runtime.NumGoroutine(); n > 10000 && !globalIsCICD {
			return nil, fmt.Errorf("unable to perform CPU IO profile with %d goroutines", n)
		}
		dirPath, err := os.MkdirTemp("", "profile")
		if err != nil {
			return nil, err
		}
		fn := filepath.Join(dirPath, "cpuio.out")
		f, err := Create(fn)
		if err != nil {
			return nil, err
		}
		stop := fgprof.Start(f, fgprof.FormatPprof)
		startedAt := time.Now()
		prof.stopFn = func() ([]byte, error) {
			if elapsed := time.Since(startedAt); elapsed < 100*time.Millisecond {
				// Light hack around https://github.com/felixge/fgprof/pull/34
				time.Sleep(100*time.Millisecond - elapsed)
			}
			err := stop()
			if err != nil {
				return nil, err
			}
			err = f.Close()
			if err != nil {
				return nil, err
			}
			defer RemoveAll(dirPath)
			return ioutilx.ReadFile(fn)
		}
	case madmin.ProfilerMEM:
		runtime.GC()
		prof.record("heap", 0, "before")
		prof.stopFn = func() ([]byte, error) {
			runtime.GC()
			var buf bytes.Buffer
			err := pprof.Lookup("heap").WriteTo(&buf, 0)
			return buf.Bytes(), err
		}
	case madmin.ProfilerBlock:
		runtime.SetBlockProfileRate(100)
		prof.stopFn = func() ([]byte, error) {
			var buf bytes.Buffer
			err := pprof.Lookup("block").WriteTo(&buf, 0)
			runtime.SetBlockProfileRate(0)
			return buf.Bytes(), err
		}
	case madmin.ProfilerMutex:
		prof.record("mutex", 0, "before")
		runtime.SetMutexProfileFraction(1)
		prof.stopFn = func() ([]byte, error) {
			var buf bytes.Buffer
			err := pprof.Lookup("mutex").WriteTo(&buf, 0)
			runtime.SetMutexProfileFraction(0)
			return buf.Bytes(), err
		}
	case madmin.ProfilerThreads:
		prof.record("threadcreate", 0, "before")
		prof.stopFn = func() ([]byte, error) {
			var buf bytes.Buffer
			err := pprof.Lookup("threadcreate").WriteTo(&buf, 0)
			return buf.Bytes(), err
		}
	case madmin.ProfilerGoroutines:
		prof.ext = "txt"
		prof.record("goroutine", 1, "before")
		prof.record("goroutine", 2, "before,debug=2")
		prof.stopFn = func() ([]byte, error) {
			var buf bytes.Buffer
			err := pprof.Lookup("goroutine").WriteTo(&buf, 1)
			return buf.Bytes(), err
		}
	case madmin.ProfilerTrace:
		dirPath, err := os.MkdirTemp("", "profile")
		if err != nil {
			return nil, err
		}
		fn := filepath.Join(dirPath, "trace.out")
		f, err := Create(fn)
		if err != nil {
			return nil, err
		}
		err = trace.Start(f)
		if err != nil {
			return nil, err
		}
		prof.ext = "trace"
		prof.stopFn = func() ([]byte, error) {
			trace.Stop()
			err := f.Close()
			if err != nil {
				return nil, err
			}
			defer RemoveAll(dirPath)
			return ioutilx.ReadFile(fn)
		}
	default:
		return nil, errors.New("profiler type unknown")
	}

	return prof, nil
}

// minioProfiler - minio profiler interface.
type minioProfiler interface {
	// Return recorded profiles, each profile associated with a distinct generic name.
	Records() map[string][]byte
	// Stop the profiler
	Stop() ([]byte, error)
	// Return extension of profile
	Extension() string
}

// Global profiler to be used by service go-routine.
var (
	globalProfiler   map[string]minioProfiler
	globalProfilerMu sync.Mutex
)

// dump the request into a string in JSON format.
func dumpRequest(r *http.Request) string {
	header := r.Header.Clone()
	header.Set("Host", r.Host)
	// Replace all '%' to '%%' so that printer format parser
	// to ignore URL encoded values.
	rawURI := strings.ReplaceAll(r.RequestURI, "%", "%%")
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
	return strings.TrimSpace(buffer.String())
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

// GetDefaultConnSettings returns default HTTP connection settings.
func GetDefaultConnSettings() xhttp.ConnSettings {
	return xhttp.ConnSettings{
		LookupHost:  globalDNSCache.LookupHost,
		DialTimeout: rest.DefaultTimeout,
		RootCAs:     globalRootCAs,
		TCPOptions:  globalTCPOptions,
	}
}

// NewInternodeHTTPTransport returns a transport for internode MinIO
// connections.
func NewInternodeHTTPTransport(maxIdleConnsPerHost int) func() http.RoundTripper {
	return xhttp.ConnSettings{
		LookupHost:       globalDNSCache.LookupHost,
		DialTimeout:      rest.DefaultTimeout,
		RootCAs:          globalRootCAs,
		CipherSuites:     crypto.TLSCiphers(),
		CurvePreferences: crypto.TLSCurveIDs(),
		EnableHTTP2:      false,
		TCPOptions:       globalTCPOptions,
	}.NewInternodeHTTPTransport(maxIdleConnsPerHost)
}

// NewHTTPTransportWithClientCerts returns a new http configuration
// used while communicating with the cloud backends.
func NewHTTPTransportWithClientCerts(clientCert, clientKey string) http.RoundTripper {
	s := xhttp.ConnSettings{
		LookupHost:       globalDNSCache.LookupHost,
		DialTimeout:      defaultDialTimeout,
		RootCAs:          globalRootCAs,
		CipherSuites:     crypto.TLSCiphersBackwardCompatible(),
		CurvePreferences: crypto.TLSCurveIDs(),
		TCPOptions:       globalTCPOptions,
		EnableHTTP2:      false,
	}

	if clientCert != "" && clientKey != "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		transport, err := s.NewHTTPTransportWithClientCerts(ctx, clientCert, clientKey)
		if err != nil {
			internalLogIf(ctx, fmt.Errorf("Unable to load client key and cert, please check your client certificate configuration: %w", err))
		}
		if transport == nil {
			// Client certs are not readable return default transport.
			return s.NewHTTPTransportWithTimeout(1 * time.Minute)
		}
		return transport
	}

	return globalRemoteTargetTransport
}

// NewHTTPTransport returns a new http configuration
// used while communicating with the cloud backends.
func NewHTTPTransport() *http.Transport {
	return NewHTTPTransportWithTimeout(1 * time.Minute)
}

// Default values for dial timeout
const defaultDialTimeout = 5 * time.Second

// NewHTTPTransportWithTimeout allows setting a timeout.
func NewHTTPTransportWithTimeout(timeout time.Duration) *http.Transport {
	return xhttp.ConnSettings{
		LookupHost:       globalDNSCache.LookupHost,
		DialTimeout:      defaultDialTimeout,
		RootCAs:          globalRootCAs,
		TCPOptions:       globalTCPOptions,
		CipherSuites:     crypto.TLSCiphersBackwardCompatible(),
		CurvePreferences: crypto.TLSCurveIDs(),
		EnableHTTP2:      false,
	}.NewHTTPTransportWithTimeout(timeout)
}

// NewRemoteTargetHTTPTransport returns a new http configuration
// used while communicating with the remote replication targets.
func NewRemoteTargetHTTPTransport(insecure bool) func() *http.Transport {
	return xhttp.ConnSettings{
		LookupHost:       globalDNSCache.LookupHost,
		RootCAs:          globalRootCAs,
		CipherSuites:     crypto.TLSCiphersBackwardCompatible(),
		CurvePreferences: crypto.TLSCurveIDs(),
		TCPOptions:       globalTCPOptions,
		EnableHTTP2:      false,
	}.NewRemoteTargetHTTPTransport(insecure)
}

// ceilFrac takes a numerator and denominator representing a fraction
// and returns its ceiling. If denominator is 0, it returns 0 instead
// of crashing.
func ceilFrac(numerator, denominator int64) (ceil int64) {
	if denominator == 0 {
		// do nothing on invalid input
		return ceil
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
	return ceil
}

// cleanMinioInternalMetadataKeys removes X-Amz-Meta- prefix from minio internal
// encryption metadata.
func cleanMinioInternalMetadataKeys(metadata map[string]string) map[string]string {
	newMeta := make(map[string]string, len(metadata))
	for k, v := range metadata {
		if strings.HasPrefix(k, "X-Amz-Meta-X-Minio-Internal-") {
			newMeta[strings.TrimPrefix(k, "X-Amz-Meta-")] = v
		} else {
			newMeta[k] = v
		}
	}
	return newMeta
}

// pathClean is like path.Clean but does not return "." for
// empty inputs, instead returns "empty" as is.
func pathClean(p string) string {
	cp := path.Clean(p)
	if cp == "." {
		return ""
	}
	return cp
}

func trimLeadingSlash(ep string) string {
	if len(ep) > 0 && ep[0] == '/' {
		// Path ends with '/' preserve it
		if ep[len(ep)-1] == '/' && len(ep) > 1 {
			ep = path.Clean(ep)
			ep += slashSeparator
		} else {
			ep = path.Clean(ep)
		}
		ep = ep[1:]
	}
	return ep
}

// unescapeGeneric is similar to url.PathUnescape or url.QueryUnescape
// depending on input, additionally also handles situations such as
// `//` are normalized as `/`, also removes any `/` prefix before
// returning.
func unescapeGeneric(p string, escapeFn func(string) (string, error)) (string, error) {
	ep, err := escapeFn(p)
	if err != nil {
		return "", err
	}
	return trimLeadingSlash(ep), nil
}

// unescapePath is similar to unescapeGeneric but for specifically
// path unescaping.
func unescapePath(p string) (string, error) {
	return unescapeGeneric(p, url.PathUnescape)
}

// similar to unescapeGeneric but never returns any error if the unescaping
// fails, returns the input as is in such occasion, not meant to be
// used where strict validation is expected.
func likelyUnescapeGeneric(p string, escapeFn func(string) (string, error)) string {
	ep, err := unescapeGeneric(p, escapeFn)
	if err != nil {
		return p
	}
	return ep
}

func updateReqContext(ctx context.Context, objects ...ObjectV) context.Context {
	req := logger.GetReqInfo(ctx)
	if req != nil {
		req.Lock()
		defer req.Unlock()
		req.Objects = make([]logger.ObjectVersion, 0, len(objects))
		for _, ov := range objects {
			req.Objects = append(req.Objects, logger.ObjectVersion{
				ObjectName: ov.ObjectName,
				VersionID:  ov.VersionID,
			})
		}
		return logger.SetReqInfo(ctx, req)
	}
	return ctx
}

// Returns context with ReqInfo details set in the context.
func newContext(r *http.Request, w http.ResponseWriter, api string) context.Context {
	reqID := w.Header().Get(xhttp.AmzRequestID)

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := likelyUnescapeGeneric(vars["object"], url.PathUnescape)
	reqInfo := &logger.ReqInfo{
		DeploymentID: globalDeploymentID(),
		RequestID:    reqID,
		RemoteHost:   handlers.GetSourceIP(r),
		Host:         getHostName(r),
		UserAgent:    r.UserAgent(),
		API:          api,
		BucketName:   bucket,
		ObjectName:   object,
		VersionID:    strings.TrimSpace(r.Form.Get(xhttp.VersionID)),
	}

	return logger.SetReqInfo(r.Context(), reqInfo)
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

// Suffix returns the longest common suffix of the provided strings
func lcpSuffix(strs []string) string {
	return lcp(strs, false)
}

func lcp(strs []string, pre bool) string {
	// short-circuit empty list
	if len(strs) == 0 {
		return ""
	}
	xfix := strs[0]
	// short-circuit single-element list
	if len(strs) == 1 {
		return xfix
	}
	// compare first to rest
	for _, str := range strs[1:] {
		xfixl := len(xfix)
		strl := len(str)
		// short-circuit empty strings
		if xfixl == 0 || strl == 0 {
			return ""
		}
		// maximum possible length
		maxl := min(strl, xfixl)
		// compare letters
		if pre {
			// prefix, iterate left to right
			for i := range maxl {
				if xfix[i] != str[i] {
					xfix = xfix[:i]
					break
				}
			}
		} else {
			// suffix, iterate right to left
			for i := range maxl {
				xi := xfixl - i - 1
				si := strl - i - 1
				if xfix[xi] != str[si] {
					xfix = xfix[xi+1:]
					break
				}
			}
		}
	}
	return xfix
}

// Returns the mode in which MinIO is running
func getMinioMode() string {
	switch {
	case globalIsDistErasure:
		return globalMinioModeDistErasure
	case globalIsErasure:
		return globalMinioModeErasure
	case globalIsErasureSD:
		return globalMinioModeErasureSD
	default:
		return globalMinioModeFS
	}
}

func iamPolicyClaimNameOpenID() string {
	return globalIAMSys.OpenIDConfig.GetIAMPolicyClaimName()
}

func iamPolicyClaimNameSA() string {
	return "sa-policy"
}

// On MinIO a directory object is stored as a regular object with "__XLDIR__" suffix.
// For ex. "prefix/" is stored as "prefix__XLDIR__"
func encodeDirObject(object string) string {
	if HasSuffix(object, slashSeparator) {
		return strings.TrimSuffix(object, slashSeparator) + globalDirSuffix
	}
	return object
}

// Reverse process of encodeDirObject()
func decodeDirObject(object string) string {
	if HasSuffix(object, globalDirSuffix) {
		return strings.TrimSuffix(object, globalDirSuffix) + slashSeparator
	}
	return object
}

func isDirObject(object string) bool {
	if obj := encodeDirObject(object); obj != object {
		object = obj
	}
	return HasSuffix(object, globalDirSuffix)
}

// Helper method to return total number of nodes in cluster
func totalNodeCount() int {
	totalNodesCount := len(globalEndpoints.Hostnames())
	if totalNodesCount == 0 {
		totalNodesCount = 1 // For standalone erasure coding
	}
	return totalNodesCount
}

// AuditLogOptions takes options for audit logging subsystem activity
type AuditLogOptions struct {
	Event     string
	APIName   string
	Status    string
	Bucket    string
	Object    string
	VersionID string
	Error     string
	Tags      map[string]string
}

// sends audit logs for internal subsystem activity
func auditLogInternal(ctx context.Context, opts AuditLogOptions) {
	if len(logger.AuditTargets()) == 0 {
		return
	}

	entry := audit.NewEntry(globalDeploymentID())
	entry.Trigger = opts.Event
	entry.Event = opts.Event
	entry.Error = opts.Error
	entry.API.Name = opts.APIName
	entry.API.Bucket = opts.Bucket
	entry.API.Objects = []xaudit.ObjectVersion{{ObjectName: opts.Object, VersionID: opts.VersionID}}
	entry.API.Status = opts.Status
	entry.Tags = make(map[string]any, len(opts.Tags))
	for k, v := range opts.Tags {
		entry.Tags[k] = v
	}

	// Merge tag information if found - this is currently needed for tags
	// set during decommissioning.
	if reqInfo := logger.GetReqInfo(ctx); reqInfo != nil {
		reqInfo.PopulateTagsMap(opts.Tags)
	}
	ctx = logger.SetAuditEntry(ctx, &entry)
	logger.AuditLog(ctx, nil, nil, nil)
}

func newTLSConfig(getCert certs.GetCertificateFunc) *tls.Config {
	if getCert == nil {
		return nil
	}

	tlsConfig := &tls.Config{
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
		NextProtos:               []string{"http/1.1", "h2"},
		GetCertificate:           getCert,
		ClientSessionCache:       tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
	}

	tlsClientIdentity := env.Get(xtls.EnvIdentityTLSEnabled, "") == config.EnableOn
	if tlsClientIdentity {
		tlsConfig.ClientAuth = tls.RequestClientCert
	}

	if secureCiphers := env.Get(api.EnvAPISecureCiphers, config.EnableOn) == config.EnableOn; secureCiphers {
		tlsConfig.CipherSuites = crypto.TLSCiphers()
	} else {
		tlsConfig.CipherSuites = crypto.TLSCiphersBackwardCompatible()
	}
	tlsConfig.CurvePreferences = crypto.TLSCurveIDs()
	return tlsConfig
}

/////////// Types and functions for OpenID IAM testing

// OpenIDClientAppParams - contains openID client application params, used in
// testing.
type OpenIDClientAppParams struct {
	ClientID, ClientSecret, ProviderURL, RedirectURL string
}

// MockOpenIDTestUserInteraction - tries to login to dex using provided credentials.
// It performs the user's browser interaction to login and retrieves the auth
// code from dex and exchanges it for a JWT.
func MockOpenIDTestUserInteraction(ctx context.Context, pro OpenIDClientAppParams, username, password string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	provider, err := oidc.NewProvider(ctx, pro.ProviderURL)
	if err != nil {
		return "", fmt.Errorf("unable to create provider: %v", err)
	}

	// Configure an OpenID Connect aware OAuth2 client.
	oauth2Config := oauth2.Config{
		ClientID:     pro.ClientID,
		ClientSecret: pro.ClientSecret,
		RedirectURL:  pro.RedirectURL,

		// Discovery returns the OAuth2 endpoints.
		Endpoint: provider.Endpoint(),

		// "openid" is a required scope for OpenID Connect flows.
		Scopes: []string{oidc.ScopeOpenID, "groups"},
	}

	state := fmt.Sprintf("x%dx", time.Now().Unix())
	authCodeURL := oauth2Config.AuthCodeURL(state)
	// fmt.Printf("authcodeurl: %s\n", authCodeURL)

	var lastReq *http.Request
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		// fmt.Printf("CheckRedirect:\n")
		// fmt.Printf("Upcoming: %s %s\n", req.Method, req.URL.String())
		// for i, c := range via {
		// 	fmt.Printf("Sofar %d: %s %s\n", i, c.Method, c.URL.String())
		// }
		// Save the last request in a redirect chain.
		lastReq = req
		// We do not follow redirect back to client application.
		if req.URL.Path == "/oauth_callback" {
			return http.ErrUseLastResponse
		}
		return nil
	}

	dexClient := http.Client{
		CheckRedirect: checkRedirect,
	}

	u, err := url.Parse(authCodeURL)
	if err != nil {
		return "", fmt.Errorf("url parse err: %v", err)
	}

	// Start the user auth flow. This page would present the login with
	// email or LDAP option.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("new request err: %v", err)
	}
	_, err = dexClient.Do(req)
	// fmt.Printf("Do: %#v %#v\n", resp, err)
	if err != nil {
		return "", fmt.Errorf("auth url request err: %v", err)
	}

	// Modify u to choose the ldap option
	u.Path += "/ldap"
	// fmt.Println(u)

	// Pick the LDAP login option. This would return a form page after
	// following some redirects. `lastReq` would be the URL of the form
	// page, where we need to POST (submit) the form.
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("new request err (/ldap): %v", err)
	}
	_, err = dexClient.Do(req)
	// fmt.Printf("Fetch LDAP login page: %#v %#v\n", resp, err)
	if err != nil {
		return "", fmt.Errorf("request err: %v", err)
	}
	// {
	// 	bodyBuf, err := io.ReadAll(resp.Body)
	// 	if err != nil {
	// 		return "", fmt.Errorf("Error reading body: %v", err)
	// 	}
	// 	fmt.Printf("bodyBuf (for LDAP login page): %s\n", string(bodyBuf))
	// }

	// Fill the login form with our test creds:
	// fmt.Printf("login form url: %s\n", lastReq.URL.String())
	formData := url.Values{}
	formData.Set("login", username)
	formData.Set("password", password)
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, lastReq.URL.String(), strings.NewReader(formData.Encode()))
	if err != nil {
		return "", fmt.Errorf("new request err (/login): %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_, err = dexClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("post form err: %v", err)
	}
	// fmt.Printf("resp: %#v %#v\n", resp.StatusCode, resp.Header)
	// bodyBuf, err := io.ReadAll(resp.Body)
	// if err != nil {
	// 	return "", fmt.Errorf("Error reading body: %v", err)
	// }
	// fmt.Printf("resp body: %s\n", string(bodyBuf))
	// fmt.Printf("lastReq: %#v\n", lastReq.URL.String())

	// On form submission, the last redirect response contains the auth
	// code, which we now have in `lastReq`. Exchange it for a JWT id_token.
	q := lastReq.URL.Query()
	// fmt.Printf("lastReq.URL: %#v q: %#v\n", lastReq.URL, q)
	code := q.Get("code")
	oauth2Token, err := oauth2Config.Exchange(ctx, code)
	if err != nil {
		return "", fmt.Errorf("unable to exchange code for id token: %v", err)
	}

	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return "", fmt.Errorf("id_token not found!")
	}

	// fmt.Printf("TOKEN: %s\n", rawIDToken)
	return rawIDToken, nil
}

// unwrapAll will unwrap the returned error completely.
func unwrapAll(err error) error {
	for {
		werr := errors.Unwrap(err)
		if werr == nil {
			return err
		}
		err = werr
	}
}

// stringsHasPrefixFold tests whether the string s begins with prefix ignoring case.
func stringsHasPrefixFold(s, prefix string) bool {
	// Test match with case first.
	return len(s) >= len(prefix) && (s[0:len(prefix)] == prefix || strings.EqualFold(s[0:len(prefix)], prefix))
}

func ptr[T any](a T) *T {
	return &a
}

// sleepContext sleeps for d duration or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}
	return nil
}

// helper type to return either item or error.
type itemOrErr[V any] struct {
	Item V
	Err  error
}

func filterStorageClass(ctx context.Context, s string) string {
	// Veeam 14.0 and later clients are not compatible with custom storage classes.
	if globalVeeamForceSC != "" && s != storageclass.STANDARD && s != storageclass.RRS && isVeeamClient(ctx) {
		return globalVeeamForceSC
	}
	return s
}

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr | ~float32 | ~float64 | string
}

// mapKeysSorted returns the map keys as a sorted slice.
func mapKeysSorted[Map ~map[K]V, K ordered, V any](m Map) []K {
	res := make([]K, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	slices.Sort(res)
	return res
}
