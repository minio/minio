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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mcontext"
	xnet "github.com/minio/pkg/v3/net"
)

const (
	copyDirective    = "COPY"
	replaceDirective = "REPLACE"
	accessDirective  = "ACCESS"
)

// Parses location constraint from the incoming reader.
func parseLocationConstraint(r *http.Request) (location string, s3Error APIErrorCode) {
	// If the request has no body with content-length set to 0,
	// we do not have to validate location constraint. Bucket will
	// be created at default region.
	locationConstraint := createBucketLocationConfiguration{}
	err := xmlDecoder(r.Body, &locationConstraint, r.ContentLength)
	if err != nil && r.ContentLength != 0 {
		internalLogOnceIf(GlobalContext, err, "location-constraint-xml-parsing")
		// Treat all other failures as XML parsing errors.
		return "", ErrMalformedXML
	} // else for both err as nil or io.EOF
	location = locationConstraint.Location
	if location == "" {
		location = globalSite.Region()
	}
	if !isValidLocation(location) {
		return location, ErrInvalidRegion
	}

	return location, ErrNone
}

// Validates input location is same as configured region
// of MinIO server.
func isValidLocation(location string) bool {
	region := globalSite.Region()
	return region == "" || region == location
}

// Supported headers that needs to be extracted.
var supportedHeaders = []string{
	"content-type",
	"cache-control",
	"content-language",
	"content-encoding",
	"content-disposition",
	"x-amz-storage-class",
	xhttp.AmzStorageClass,
	xhttp.AmzObjectTagging,
	"expires",
	xhttp.AmzBucketReplicationStatus,
	"X-Minio-Replication-Server-Side-Encryption-Sealed-Key",
	"X-Minio-Replication-Server-Side-Encryption-Seal-Algorithm",
	"X-Minio-Replication-Server-Side-Encryption-Iv",
	"X-Minio-Replication-Encrypted-Multipart",
	"X-Minio-Replication-Actual-Object-Size",
	ReplicationSsecChecksumHeader,
	// Add more supported headers here.
}

// mapping of internal headers to allowed replication headers
var validSSEReplicationHeaders = map[string]string{
	"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "X-Minio-Replication-Server-Side-Encryption-Sealed-Key",
	"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "X-Minio-Replication-Server-Side-Encryption-Seal-Algorithm",
	"X-Minio-Internal-Server-Side-Encryption-Iv":             "X-Minio-Replication-Server-Side-Encryption-Iv",
	"X-Minio-Internal-Encrypted-Multipart":                   "X-Minio-Replication-Encrypted-Multipart",
	"X-Minio-Internal-Actual-Object-Size":                    "X-Minio-Replication-Actual-Object-Size",
	// Add more supported headers here.
}

// mapping of replication headers to internal headers
var replicationToInternalHeaders = map[string]string{
	"X-Minio-Replication-Server-Side-Encryption-Sealed-Key":     "X-Minio-Internal-Server-Side-Encryption-Sealed-Key",
	"X-Minio-Replication-Server-Side-Encryption-Seal-Algorithm": "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm",
	"X-Minio-Replication-Server-Side-Encryption-Iv":             "X-Minio-Internal-Server-Side-Encryption-Iv",
	"X-Minio-Replication-Encrypted-Multipart":                   "X-Minio-Internal-Encrypted-Multipart",
	"X-Minio-Replication-Actual-Object-Size":                    "X-Minio-Internal-Actual-Object-Size",
	ReplicationSsecChecksumHeader:                               ReplicationSsecChecksumHeader,
	// Add more supported headers here.
}

// isDirectiveValid - check if tagging-directive is valid.
func isDirectiveValid(v string) bool {
	// Check if set metadata-directive is valid.
	return isDirectiveCopy(v) || isDirectiveReplace(v)
}

// Check if the directive COPY is requested.
func isDirectiveCopy(value string) bool {
	// By default if directive is not set we
	// treat it as 'COPY' this function returns true.
	return value == copyDirective || value == ""
}

// Check if the directive REPLACE is requested.
func isDirectiveReplace(value string) bool {
	return value == replaceDirective
}

// userMetadataKeyPrefixes contains the prefixes of used-defined metadata keys.
// All values stored with a key starting with one of the following prefixes
// must be extracted from the header.
var userMetadataKeyPrefixes = []string{
	"x-amz-meta-",
	"x-minio-meta-",
}

// extractMetadataFromReq extracts metadata from HTTP header and HTTP queryString.
func extractMetadataFromReq(ctx context.Context, r *http.Request) (metadata map[string]string, err error) {
	return extractMetadata(ctx, textproto.MIMEHeader(r.Form), textproto.MIMEHeader(r.Header))
}

func extractMetadata(ctx context.Context, mimesHeader ...textproto.MIMEHeader) (metadata map[string]string, err error) {
	metadata = make(map[string]string)

	for _, hdr := range mimesHeader {
		// Extract all query values.
		err = extractMetadataFromMime(ctx, hdr, metadata)
		if err != nil {
			return nil, err
		}
	}

	// Set content-type to default value if it is not set.
	if _, ok := metadata[strings.ToLower(xhttp.ContentType)]; !ok {
		metadata[strings.ToLower(xhttp.ContentType)] = "binary/octet-stream"
	}

	// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
	for k := range metadata {
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			delete(metadata, k)
		}
	}

	if contentEncoding, ok := metadata[strings.ToLower(xhttp.ContentEncoding)]; ok {
		contentEncoding = trimAwsChunkedContentEncoding(contentEncoding)
		if contentEncoding != "" {
			// Make sure to trim and save the content-encoding
			// parameter for a streaming signature which is set
			// to a custom value for example: "aws-chunked,gzip".
			metadata[strings.ToLower(xhttp.ContentEncoding)] = contentEncoding
		} else {
			// Trimmed content encoding is empty when the header
			// value is set to "aws-chunked" only.

			// Make sure to delete the content-encoding parameter
			// for a streaming signature which is set to value
			// for example: "aws-chunked"
			delete(metadata, strings.ToLower(xhttp.ContentEncoding))
		}
	}

	// Success.
	return metadata, nil
}

// extractMetadata extracts metadata from map values.
func extractMetadataFromMime(ctx context.Context, v textproto.MIMEHeader, m map[string]string) error {
	if v == nil {
		bugLogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	nv := make(textproto.MIMEHeader, len(v))
	for k, kv := range v {
		// Canonicalize all headers, to remove any duplicates.
		nv[http.CanonicalHeaderKey(k)] = kv
	}

	// Save all supported headers.
	for _, supportedHeader := range supportedHeaders {
		value, ok := nv[http.CanonicalHeaderKey(supportedHeader)]
		if ok {
			if v, ok := replicationToInternalHeaders[supportedHeader]; ok {
				m[v] = strings.Join(value, ",")
			} else {
				m[supportedHeader] = strings.Join(value, ",")
			}
		}
	}

	for key := range v {
		for _, prefix := range userMetadataKeyPrefixes {
			if !stringsHasPrefixFold(key, prefix) {
				continue
			}
			value, ok := nv[http.CanonicalHeaderKey(key)]
			if ok {
				m[key] = strings.Join(value, ",")
				break
			}
		}
	}
	return nil
}

// Returns access credentials in the request Authorization header.
func getReqAccessCred(r *http.Request, region string) (cred auth.Credentials) {
	cred, _, _ = getReqAccessKeyV4(r, region, serviceS3)
	if cred.AccessKey == "" {
		cred, _, _ = getReqAccessKeyV2(r)
	}
	return cred
}

// Extract request params to be sent with event notification.
func extractReqParams(r *http.Request) map[string]string {
	if r == nil {
		return nil
	}

	region := globalSite.Region()
	cred := getReqAccessCred(r, region)

	principalID := cred.AccessKey
	if cred.ParentUser != "" {
		principalID = cred.ParentUser
	}

	// Success.
	m := map[string]string{
		"region":          region,
		"principalId":     principalID,
		"sourceIPAddress": handlers.GetSourceIP(r),
		// Add more fields here.
	}
	if rangeField := r.Header.Get(xhttp.Range); rangeField != "" {
		m["range"] = rangeField
	}

	if _, ok := r.Header[xhttp.MinIOSourceReplicationRequest]; ok {
		m[xhttp.MinIOSourceReplicationRequest] = ""
	}
	return m
}

// Extract response elements to be sent with event notification.
func extractRespElements(w http.ResponseWriter) map[string]string {
	if w == nil {
		return map[string]string{}
	}
	return map[string]string{
		"requestId":      w.Header().Get(xhttp.AmzRequestID),
		"nodeId":         w.Header().Get(xhttp.AmzRequestHostID),
		"content-length": w.Header().Get(xhttp.ContentLength),
		// Add more fields here.
	}
}

// Trims away `aws-chunked` from the content-encoding header if present.
// Streaming signature clients can have custom content-encoding such as
// `aws-chunked,gzip` here we need to only save `gzip`.
// For more refer http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
func trimAwsChunkedContentEncoding(contentEnc string) (trimmedContentEnc string) {
	if contentEnc == "" {
		return contentEnc
	}
	var newEncs []string
	for enc := range strings.SplitSeq(contentEnc, ",") {
		if enc != streamingContentEncoding {
			newEncs = append(newEncs, enc)
		}
	}
	return strings.Join(newEncs, ",")
}

func collectInternodeStats(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.ServeHTTP(w, r)

		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if !ok || tc == nil {
			return
		}

		globalConnStats.incInternodeInputBytes(int64(tc.RequestRecorder.Size()))
		globalConnStats.incInternodeOutputBytes(int64(tc.ResponseRecorder.Size()))
	}
}

func collectAPIStats(api string, f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resource, err := getResource(r.URL.Path, r.Host, globalDomainNames)
		if err != nil {
			defer logger.AuditLog(r.Context(), w, r, mustGetClaimsFromToken(r))

			apiErr := errorCodes.ToAPIErr(ErrUnsupportedHostHeader)
			apiErr.Description = fmt.Sprintf("%s: %v", apiErr.Description, err)

			writeErrorResponse(r.Context(), w, apiErr, r.URL)
			return
		}

		bucket, _ := path2BucketObject(resource)

		meta, err := globalBucketMetadataSys.Get(bucket) // check if this bucket exists.
		countBktStat := bucket != "" && bucket != minioReservedBucket && err == nil && !meta.Created.IsZero()
		if countBktStat {
			globalBucketHTTPStats.updateHTTPStats(bucket, api, nil)
		}

		globalHTTPStats.currentS3Requests.Inc(api)
		f.ServeHTTP(w, r)
		globalHTTPStats.currentS3Requests.Dec(api)

		tc, _ := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if tc != nil {
			globalHTTPStats.updateStats(api, tc.ResponseRecorder)
			globalConnStats.incS3InputBytes(int64(tc.RequestRecorder.Size()))
			globalConnStats.incS3OutputBytes(int64(tc.ResponseRecorder.Size()))

			if countBktStat {
				globalBucketConnStats.incS3InputBytes(bucket, int64(tc.RequestRecorder.Size()))
				globalBucketConnStats.incS3OutputBytes(bucket, int64(tc.ResponseRecorder.Size()))
				globalBucketHTTPStats.updateHTTPStats(bucket, api, tc.ResponseRecorder)
			}
		}
	}
}

// Returns "/bucketName/objectName" for path-style or virtual-host-style requests.
func getResource(path string, host string, domains []string) (string, error) {
	if len(domains) == 0 {
		return path, nil
	}

	// If virtual-host-style is enabled construct the "resource" properly.
	xhost, err := xnet.ParseHost(host)
	if err != nil {
		return "", err
	}

	for _, domain := range domains {
		if xhost.Name == minioReservedBucket+"."+domain {
			continue
		}
		if !strings.HasSuffix(xhost.Name, "."+domain) {
			continue
		}
		bucket := strings.TrimSuffix(xhost.Name, "."+domain)
		return SlashSeparator + pathJoin(bucket, path), nil
	}
	return path, nil
}

var regexVersion = regexp.MustCompile(`^/minio.*/(v\d+)/.*`)

func extractAPIVersion(r *http.Request) string {
	if matches := regexVersion.FindStringSubmatch(r.URL.Path); len(matches) > 1 {
		return matches[1]
	}
	return "unknown"
}

func methodNotAllowedHandler(api string) func(w http.ResponseWriter, r *http.Request) {
	return errorResponseHandler
}

// If none of the http routes match respond with appropriate errors
func errorResponseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		return
	}
	desc := "Do not upgrade one server at a time - please follow the recommended guidelines mentioned here https://github.com/minio/minio#upgrading-minio for your environment"
	switch {
	case strings.HasPrefix(r.URL.Path, peerRESTPrefix):
		writeErrorResponseString(r.Context(), w, APIError{
			Code:           "XMinioPeerVersionMismatch",
			Description:    desc,
			HTTPStatusCode: http.StatusUpgradeRequired,
		}, r.URL)
	case strings.HasPrefix(r.URL.Path, storageRESTPrefix):
		writeErrorResponseString(r.Context(), w, APIError{
			Code:           "XMinioStorageVersionMismatch",
			Description:    desc,
			HTTPStatusCode: http.StatusUpgradeRequired,
		}, r.URL)
	case strings.HasPrefix(r.URL.Path, adminPathPrefix):
		var desc string
		version := extractAPIVersion(r)
		switch version {
		case "v1", madmin.AdminAPIVersionV2:
			desc = fmt.Sprintf("Server expects client requests with 'admin' API version '%s', found '%s', please upgrade the client to latest releases", madmin.AdminAPIVersion, version)
		case madmin.AdminAPIVersion:
			desc = fmt.Sprintf("This 'admin' API is not supported by server in '%s'", getMinioMode())
		default:
			desc = fmt.Sprintf("Unexpected client 'admin' API version found '%s', expected '%s', please downgrade the client to older releases", version, madmin.AdminAPIVersion)
		}
		writeErrorResponseJSON(r.Context(), w, APIError{
			Code:           "XMinioAdminVersionMismatch",
			Description:    desc,
			HTTPStatusCode: http.StatusUpgradeRequired,
		}, r.URL)
	default:
		defer logger.AuditLog(r.Context(), w, r, mustGetClaimsFromToken(r))
		defer atomic.AddUint64(&globalHTTPStats.rejectedRequestsInvalid, 1)

		// When we are not running in S3 Express mode, generate appropriate error
		// for x-amz-write-offset HEADER specified.
		if _, ok := r.Header[xhttp.AmzWriteOffsetBytes]; ok {
			tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
			if ok {
				tc.FuncName = "s3.AppendObject"
				tc.ResponseRecorder.LogErrBody = true
			}

			writeErrorResponse(r.Context(), w, getAPIError(ErrNotImplemented), r.URL)
			return
		}

		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if ok {
			tc.FuncName = "s3.ValidRequest"
			tc.ResponseRecorder.LogErrBody = true
		}

		writeErrorResponse(r.Context(), w, APIError{
			Code: "BadRequest",
			Description: fmt.Sprintf("An unsupported API call for method: %s at '%s'",
				r.Method, r.URL.Path),
			HTTPStatusCode: http.StatusBadRequest,
		}, r.URL)
	}
}

// gets host name for current node
func getHostName(r *http.Request) (hostName string) {
	if globalIsDistErasure {
		hostName = globalLocalNodeName
	} else {
		hostName = r.Host
	}
	return hostName
}

// Proxy any request to an endpoint.
func proxyRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, ep ProxyEndpoint, returnErr bool) (success bool) {
	success = true

	// Make sure we remove any existing headers before
	// proxying the request to another node.
	for k := range w.Header() {
		w.Header().Del(k)
	}

	f := handlers.NewForwarder(&handlers.Forwarder{
		PassHost:     true,
		RoundTripper: ep.Transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			success = false
			if err != nil && !errors.Is(err, context.Canceled) {
				proxyLogIf(GlobalContext, err)
			}
			if returnErr {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			}
		},
	})

	r.URL.Scheme = "http"
	if globalIsTLS {
		r.URL.Scheme = "https"
	}

	r.URL.Host = ep.Host
	f.ServeHTTP(w, r)
	return success
}
