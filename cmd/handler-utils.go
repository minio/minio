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
	"net/http"
	"net/url"
	"strings"
)

// Validates location constraint in PutBucket request body.
// The location value in the request body should match the
// region configured at serverConfig, otherwise error is returned.
func isValidLocationConstraint(r *http.Request) (s3Error APIErrorCode) {
	serverRegion := serverConfig.GetRegion()
	// If the request has no body with content-length set to 0,
	// we do not have to validate location constraint. Bucket will
	// be created at default region.
	locationConstraint := createBucketLocationConfiguration{}
	err := xmlDecoder(r.Body, &locationConstraint, r.ContentLength)
	if err == nil || err == io.EOF {
		// Successfully decoded, proceed to verify the region.
		// Once region has been obtained we proceed to verify it.
		incomingRegion := locationConstraint.Location
		if incomingRegion == "" {
			// Location constraint is empty for region globalMinioDefaultRegion,
			// in accordance with protocol.
			incomingRegion = globalMinioDefaultRegion
		}
		// Return errInvalidRegion if location constraint does not match
		// with configured region.
		s3Error = ErrNone
		if serverRegion != incomingRegion {
			s3Error = ErrInvalidRegion
		}
		return s3Error
	}
	errorIf(err, "Unable to xml decode location constraint")
	// Treat all other failures as XML parsing errors.
	return ErrMalformedXML
}

// Supported headers that needs to be extracted.
var supportedHeaders = []string{
	"content-type",
	"cache-control",
	"content-encoding",
	"content-disposition",
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

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header) map[string]string {
	if header == nil {
		return nil
	}
	metadata := make(map[string]string)
	// Save standard supported headers.
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
		cKey := http.CanonicalHeaderKey(key)
		if strings.HasPrefix(cKey, "X-Amz-Meta-") {
			metadata[cKey] = header.Get(key)
		} else if strings.HasPrefix(key, "X-Minio-Meta-") {
			metadata[cKey] = header.Get(key)
		}
	}

	// Success.
	return metadata
}

// The Query string for the redirect URL the client is
// redirected on successful upload.
func getRedirectPostRawQuery(objInfo ObjectInfo) string {
	redirectValues := make(url.Values)
	redirectValues.Set("bucket", objInfo.Bucket)
	redirectValues.Set("key", objInfo.Name)
	redirectValues.Set("etag", "\""+objInfo.MD5Sum+"\"")
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

// extractMetadataFromForm extracts metadata from Post Form.
func extractMetadataFromForm(formValues http.Header) map[string]string {
	return extractMetadataFromHeader(formValues)
}

// Validate form field size for s3 specification requirement.
func validateFormFieldSize(formValues http.Header) error {
	// Iterate over form values
	for k := range formValues {
		// Check if value's field exceeds S3 limit
		if int64(len(formValues.Get(k))) > maxFormFieldSize {
			return traceError(errSizeUnexpected)
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
				return nil, "", 0, nil, traceError(errInvalidArgument)
			}
			// Fetch fileHeader which has the uploaded file information
			fileHeader := v[0]
			// Set filename
			fileName = fileHeader.Filename
			// Open the uploaded part
			filePart, err = fileHeader.Open()
			if err != nil {
				return nil, "", 0, nil, traceError(err)
			}
			// Compute file size
			fileSize, err = filePart.(io.Seeker).Seek(0, 2)
			if err != nil {
				return nil, "", 0, nil, traceError(err)
			}
			// Reset Seek to the beginning
			_, err = filePart.(io.Seeker).Seek(0, 0)
			if err != nil {
				return nil, "", 0, nil, traceError(err)
			}
			// File found and ready for reading
			break
		}
	}

	return filePart, fileName, fileSize, formValues, nil
}
