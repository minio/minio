/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
)

const (
	// TODO: check maximum versioning document policy
	// As per AWS S3 specification, 20KiB versioning JSON data is allowed.
	maxBucketVersioningSize = 20 * humanize.KiByte

	// Versioning configuration file.
	bucketVersioningConfig = "versioning.json"
)

type VersioningConfiguration struct {
	XMLNS     string `xml:"xmlns,attr"`
	Status    string `xml:"Status,omitempty"`
	MfaDelete string `xml:"MfaDelete,omitempty"`
}

// PutBucketVersioningHandler - This HTTP handler stores given bucket versioning configuration as per
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTVersioningStatus.html
func (api objectAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketVersioning")

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	// Only allow versioning to be enabled for erasure code backend
	if !globalIsXL {
		writeErrorResponse(w, ErrBadRequest, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	//FIXME: check if get versioning has anything to do with policy configuration
	if s3Error := checkRequestAuthType(ctx, r, "", bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// PutBucketVersioning always needs Content-Length, error out if not
	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	} else if r.ContentLength > maxBucketVersioningSize {
		// Error out if Content-Length is beyond allowed size
		writeErrorResponse(w, ErrEntityTooLarge, r.URL)
		return
	}

	var versioningConfig VersioningConfiguration

	if versioningConfigBytes, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength)); err != nil {
		writeErrorResponse(w, ErrBadRequest, r.URL)
		return
	} else {
		err = xml.Unmarshal(versioningConfigBytes, &versioningConfig)
		if err != nil {
			writeErrorResponse(w, ErrMalformedPolicy, r.URL)
			return
		}
	}

	// Minio only allows versioning to be enabled (effectively just once),
	// thereafter it cannot be suspended.
	if versioningConfig.Status != "Enabled" {
		writeErrorResponse(w, ErrMalformedPolicy, r.URL)
		return
	}

	// Make sure that the bucket is empty in order to allow versioning to be enabled
	result, err := objAPI.ListObjects(ctx, bucket, "", "", "", 1)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	} else if len(result.Objects) > 0 {
		writeErrorResponse(w, ErrBucketMustBeEmpty, r.URL)
		return
	}

	if err = objAPI.SetBucketVersioning(ctx, bucket, versioningConfig); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	globalVersioningSys.Set(bucket, versioningConfig)
	// Update status of other nodes
	globalNotificationSys.SetBucketVersioning(ctx, bucket, versioningConfig)

	// Success.
	writeSuccessNoContent(w)
}

// GetBucketVersioningHandler - This HTTP handler returns bucket versioning configuration.
func (api objectAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {

	ctx := newContext(r, w, "GetBucketVersioning")

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// FIXME: fix me if setting versioning is influenced by specific API policy
	if s3Error := checkRequestAuthType(ctx, r, "", bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Read bucket versioning configuration
	versioning, err := objAPI.GetBucketVersioning(ctx, bucket)
	if err != nil {
		if _, ok := err.(BucketVersioningNotFound); !ok {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}
	}

	versioningData, err := xml.Marshal(versioning)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Write to client.
	w.Write(versioningData)
}
