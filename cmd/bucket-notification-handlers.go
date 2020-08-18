/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"reflect"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
)

const (
	bucketConfigPrefix       = "buckets"
	bucketNotificationConfig = "notification.xml"
)

// GetBucketNotificationHandler - This HTTP handler returns event notification configuration
// as per http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html.
// It returns empty configuration if its not set.
func (api objectAPIHandlers) GetBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketNotification")

	defer logger.AuditLog(w, r, "GetBucketNotification", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	_, err := objAPI.GetBucketInfo(ctx, bucketName)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := globalBucketMetadataSys.GetNotificationConfig(bucketName)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	config.SetRegion(globalServerRegion)
	if err = config.Validate(globalServerRegion, globalNotificationSys.targetList); err != nil {
		arnErr, ok := err.(*event.ErrARNNotFound)
		if ok {
			for i, queue := range config.QueueList {
				// Remove ARN not found queues, because we previously allowed
				// adding unexpected entries into the config.
				//
				// With newer config disallowing changing / turning off
				// notification targets without removing ARN in notification
				// configuration we won't see this problem anymore.
				if reflect.DeepEqual(queue.ARN, arnErr.ARN) && i < len(config.QueueList) {
					config.QueueList = append(config.QueueList[:i],
						config.QueueList[i+1:]...)
				}
				// This is a one time activity we shall do this
				// here and allow stale ARN to be removed. We shall
				// never reach a stage where we will have stale
				// notification configs.
			}
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeSuccessResponseXML(w, configData)
}

// PutBucketNotificationHandler - This HTTP handler stores given notification configuration as per
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html.
func (api objectAPIHandlers) PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketNotification")

	defer logger.AuditLog(w, r, "PutBucketNotification", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if !objectAPI.IsNotificationSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	_, err := objectAPI.GetBucketInfo(ctx, bucketName)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// PutBucketNotification always needs a Content-Length.
	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := event.ParseConfig(io.LimitReader(r.Body, r.ContentLength), globalServerRegion, globalNotificationSys.targetList)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		if event.IsEventError(err) {
			apiErr = toAPIError(ctx, err)
		}
		writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err = globalBucketMetadataSys.Update(bucketName, bucketNotificationConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rulesMap := config.ToRulesMap()
	globalNotificationSys.AddRulesMap(bucketName, rulesMap)

	writeSuccessResponseHeadersOnly(w)
}
