/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"io/ioutil"
	"mime/multipart"
	"net/http"
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
			// Location constraint is empty for region "us-east-1",
			// in accordance with protocol.
			incomingRegion = "us-east-1"
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

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header) map[string]string {
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
			metadata[cKey] = header.Get(cKey)
		} else if strings.HasPrefix(key, "X-Minio-Meta-") {
			metadata[cKey] = header.Get(cKey)
		}
	}
	// Return.
	return metadata
}

// Extract form fields and file data from a HTTP POST Policy
func extractPostPolicyFormValues(reader *multipart.Reader) (filePart io.Reader, fileName string, formValues map[string]string, err error) {
	/// HTML Form values
	formValues = make(map[string]string)
	fileName = ""
	for err == nil {
		var part *multipart.Part
		part, err = reader.NextPart()
		if part != nil {
			canonicalFormName := http.CanonicalHeaderKey(part.FormName())
			if canonicalFormName != "File" {
				var buffer []byte
				limitReader := io.LimitReader(part, maxFormFieldSize+1)
				buffer, err = ioutil.ReadAll(limitReader)
				if err != nil {
					return nil, "", nil, err
				}
				if int64(len(buffer)) > maxFormFieldSize {
					return nil, "", nil, errSizeUnexpected
				}
				formValues[canonicalFormName] = string(buffer)
			} else {
				filePart = part
				fileName = part.FileName()
				// As described in S3 spec, we expect file to be the last form field
				break
			}
		}
	}
	return filePart, fileName, formValues, nil
}
