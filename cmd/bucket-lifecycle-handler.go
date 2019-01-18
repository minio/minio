/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018, 2019 Minio, Inc.
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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lifecycle"
)

const (
	// LifeCycle configuration file.
	bucketLifeCycleConfig = "lifecycle.xml"
)

// PutBucketLifeCycleHandler - This HTTP handler stores given bucket lifecycle configuration as per
// https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html
func (api objectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketLifeCycle")

	defer logger.AuditLog(w, r, "PutBucketLifeCycle", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, lifecycle.PutBucketLifeCycleAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// PutBucketLifecycle always needs a Content-Md5
	if _, ok := r.Header["Content-Md5"]; !ok {
		writeErrorResponse(w, ErrMissingContentMD5, r.URL, guessIsBrowserReq(r))
		return
	}

	bucketLifeCycle, err := lifecycle.ParseConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		writeErrorResponse(w, ErrMalformedPolicy, r.URL, guessIsBrowserReq(r))
		return
	}

	if err = objAPI.SetBucketLifeCycle(ctx, bucket, bucketLifeCycle); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	globalLifeCycleSys.Set(bucket, *bucketLifeCycle)
	// globalNotificationSys.SetBucketPolicy(ctx, bucket, bucketPolicy)

	// Success.
	writeSuccessNoContent(w)
}

// GetBucketLifeCycleHandler - This HTTP handler returns bucket policy configuration.
func (api objectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketLifeCycle")

	defer logger.AuditLog(w, r, "GetBucketLifeCycle", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, lifecycle.GetBucketLifeCycleAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Read bucket access lifecycle.
	bucketLifeCycle, err := objAPI.GetBucketLifeCycle(ctx, bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	lifecycleData, err := xml.Marshal(bucketLifeCycle)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write to client.
	w.Write(lifecycleData)
}

// DeleteBucketLifeCycleHandler - This HTTP handler removes bucket lifecycle configuration.
func (api objectAPIHandlers) DeleteBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketLifeCycle")

	defer logger.AuditLog(w, r, "DeleteBucketLifeCycle", mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, lifecycle.DeleteBucketLifeCycleAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if bucket exists.
	if _, err := objAPI.GetBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err := objAPI.DeleteBucketLifeCycle(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	globalLifeCycleSys.Remove(bucket)
	//globalNotificationSys.RemoveBucketPolicy(ctx, bucket)

	// Success.
	writeSuccessNoContent(w)
}
