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
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio/pkg/errors"
	httptracer "github.com/minio/minio/pkg/handlers"
)

// Parses location constraint from the incoming reader.
func parseLocationConstraint(r *http.Request) (location string, s3Error APIErrorCode) {
	// If the request has no body with content-length set to 0,
	// we do not have to validate location constraint. Bucket will
	// be created at default region.
	locationConstraint := createBucketLocationConfiguration{}
	err := xmlDecoder(r.Body, &locationConstraint, r.ContentLength)
	if err != nil && err != io.EOF {
		errorIf(err, "Unable to xml decode location constraint")
		// Treat all other failures as XML parsing errors.
		return "", ErrMalformedXML
	} // else for both err as nil or io.EOF
	location = locationConstraint.Location
	if location == "" {
		location = globalServerConfig.GetRegion()
	}
	return location, ErrNone
}

// Validates input location is same as configured region
// of Minio server.
func isValidLocation(location string) bool {
	return globalServerConfig.GetRegion() == "" || globalServerConfig.GetRegion() == location
}

// Supported headers that needs to be extracted.
var supportedHeaders = []string{
	"content-type",
	"cache-control",
	"content-encoding",
	"content-disposition",
	amzStorageClass,
	// Add more supported headers here.
}

// isMetadataDirectiveValid - check if metadata-directive is valid.
func isMetadataDirectiveValid(h http.Header) bool {
	_, ok := h[http.CanonicalHeaderKey("X-Amz-Metadata-Directive")]
	if ok {
		// Check atleast set metadata-directive is valid.
		return (isMetadataCopy(h) || isMetadataReplace(h))
	}
	// By default if x-amz-metadata-directive is not we
	// treat it as 'COPY' this function returns true.
	return true
}

// Check if the metadata COPY is requested.
func isMetadataCopy(h http.Header) bool {
	return h.Get("X-Amz-Metadata-Directive") == "COPY"
}

// Check if the metadata REPLACE is requested.
func isMetadataReplace(h http.Header) bool {
	return h.Get("X-Amz-Metadata-Directive") == "REPLACE"
}

// Splits an incoming path into bucket and object components.
func path2BucketAndObject(path string) (bucket, object string) {
	// Skip the first element if it is '/', split the rest.
	path = strings.TrimPrefix(path, "/")
	pathComponents := strings.SplitN(path, "/", 2)

	// Save the bucket and object extracted from path.
	switch len(pathComponents) {
	case 1:
		bucket = pathComponents[0]
	case 2:
		bucket = pathComponents[0]
		object = pathComponents[1]
	}
	return bucket, object
}

// userMetadataKeyPrefixes contains the prefixes of used-defined metadata keys.
// All values stored with a key starting with one of the following prefixes
// must be extracted from the header.
var userMetadataKeyPrefixes = []string{
	"X-Amz-Meta-",
	"X-Minio-Meta-",
}

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header) (map[string]string, error) {
	if header == nil {
		return nil, errors.Trace(errInvalidArgument)
	}
	metadata := make(map[string]string)

	// Save all supported headers.
	for _, supportedHeader := range supportedHeaders {
		canonicalHeader := http.CanonicalHeaderKey(supportedHeader)
		// HTTP headers are case insensitive, look for both canonical
		// and non canonical entries.
		if _, ok := header[canonicalHeader]; ok {
			metadata[supportedHeader] = header.Get(canonicalHeader)
		} else if _, ok := header[supportedHeader]; ok {
			metadata[supportedHeader] = header.Get(supportedHeader)
		}
	}

	// Go through all other headers for any additional headers that needs to be saved.
	for key := range header {
		if key != http.CanonicalHeaderKey(key) {
			return nil, errors.Trace(errInvalidArgument)
		}
		for _, prefix := range userMetadataKeyPrefixes {
			if strings.HasPrefix(key, prefix) {
				metadata[key] = header.Get(key)
				break
			}
		}
	}
	return metadata, nil
}

// The Query string for the redirect URL the client is
// redirected on successful upload.
func getRedirectPostRawQuery(objInfo ObjectInfo) string {
	redirectValues := make(url.Values)
	redirectValues.Set("bucket", objInfo.Bucket)
	redirectValues.Set("key", objInfo.Name)
	redirectValues.Set("etag", "\""+objInfo.ETag+"\"")
	return redirectValues.Encode()
}

// Extract request params to be sent with event notifiation.
func extractReqParams(r *http.Request) map[string]string {
	if r == nil {
		return nil
	}

	// Success.
	return map[string]string{
		"sourceIPAddress": r.RemoteAddr,
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
	for _, enc := range strings.Split(contentEnc, ",") {
		if enc != streamingContentEncoding {
			newEncs = append(newEncs, enc)
		}
	}
	return strings.Join(newEncs, ",")
}

// Validate form field size for s3 specification requirement.
func validateFormFieldSize(formValues http.Header) error {
	// Iterate over form values
	for k := range formValues {
		// Check if value's field exceeds S3 limit
		if int64(len(formValues.Get(k))) > maxFormFieldSize {
			return errors.Trace(errSizeUnexpected)
		}
	}

	// Success.
	return nil
}

// Extract form fields and file data from a HTTP POST Policy
func extractPostPolicyFormValues(form *multipart.Form) (filePart io.ReadCloser, fileName string, fileSize int64, formValues http.Header, err error) {
	/// HTML Form values
	fileName = ""

	// Canonicalize the form values into http.Header.
	formValues = make(http.Header)
	for k, v := range form.Value {
		formValues[http.CanonicalHeaderKey(k)] = v
	}

	// Validate form values.
	if err = validateFormFieldSize(formValues); err != nil {
		return nil, "", 0, nil, err
	}

	// Iterator until we find a valid File field and break
	for k, v := range form.File {
		canonicalFormName := http.CanonicalHeaderKey(k)
		if canonicalFormName == "File" {
			if len(v) == 0 {
				return nil, "", 0, nil, errors.Trace(errInvalidArgument)
			}
			// Fetch fileHeader which has the uploaded file information
			fileHeader := v[0]
			// Set filename
			fileName = fileHeader.Filename
			// Open the uploaded part
			filePart, err = fileHeader.Open()
			if err != nil {
				return nil, "", 0, nil, errors.Trace(err)
			}
			// Compute file size
			fileSize, err = filePart.(io.Seeker).Seek(0, 2)
			if err != nil {
				return nil, "", 0, nil, errors.Trace(err)
			}
			// Reset Seek to the beginning
			_, err = filePart.(io.Seeker).Seek(0, 0)
			if err != nil {
				return nil, "", 0, nil, errors.Trace(err)
			}
			// File found and ready for reading
			break
		}
	}

	return filePart, fileName, fileSize, formValues, nil
}

// Log headers and body.
func httpTraceAll(f http.HandlerFunc) http.HandlerFunc {
	if !globalHTTPTrace {
		return f
	}
	return httptracer.TraceReqHandlerFunc(f, os.Stdout, true)
}

// Log only the headers.
func httpTraceHdrs(f http.HandlerFunc) http.HandlerFunc {
	if !globalHTTPTrace {
		return f
	}
	return httptracer.TraceReqHandlerFunc(f, os.Stdout, false)
}

// Returns "/bucketName/objectName" for path-style or virtual-host-style requests.
func getResource(path string, host string, domain string) (string, error) {
	if domain == "" {
		return path, nil
	}
	// If virtual-host-style is enabled construct the "resource" properly.
	if strings.Contains(host, ":") {
		// In bucket.mydomain.com:9000, strip out :9000
		var err error
		if host, _, err = net.SplitHostPort(host); err != nil {
			errorIf(err, "Unable to split %s", host)
			return "", err
		}
	}
	if !strings.HasSuffix(host, "."+domain) {
		return path, nil
	}
	bucket := strings.TrimSuffix(host, "."+domain)
	return slashSeparator + pathJoin(bucket, path), nil
}

// If none of the http routes match respond with MethodNotAllowed
func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(w, ErrMethodNotAllowed, r.URL)
	return
}
