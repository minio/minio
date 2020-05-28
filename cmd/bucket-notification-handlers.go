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
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/cmd/http"
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

func (api objectAPIHandlers) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListenBucketNotification")

	defer logger.AuditLog(w, r, "ListenBucketNotification", mustGetClaimsFromToken(r))

	// Validate if bucket exists.
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	if !objAPI.IsListenBucketSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	values := r.URL.Query()
	values.Set(peerRESTListenBucket, bucketName)

	var prefix string
	if len(values[peerRESTListenPrefix]) > 1 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrFilterNamePrefix), r.URL, guessIsBrowserReq(r))
		return
	}

	if len(values[peerRESTListenPrefix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenPrefix][0]); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		prefix = values[peerRESTListenPrefix][0]
	}

	var suffix string
	if len(values[peerRESTListenSuffix]) > 1 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrFilterNameSuffix), r.URL, guessIsBrowserReq(r))
		return
	}

	if len(values[peerRESTListenSuffix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenSuffix][0]); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		suffix = values[peerRESTListenSuffix][0]
	}

	pattern := event.NewPattern(prefix, suffix)

	var eventNames []event.Name
	for _, s := range values[peerRESTListenEvents] {
		eventName, err := event.ParseName(s)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		eventNames = append(eventNames, eventName)
	}

	if _, err := objAPI.GetBucketInfo(ctx, bucketName); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	w.Header().Set(xhttp.ContentType, "text/event-stream")

	// Listen Publisher and peer-listen-client uses nonblocking send and hence does not wait for slow receivers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	listenCh := make(chan interface{}, 4000)

	peers := newPeerRestClients(globalEndpoints)

	globalHTTPListen.Subscribe(listenCh, ctx.Done(), func(evI interface{}) bool {
		ev, ok := evI.(event.Event)
		if !ok {
			return false
		}
		if ev.S3.Bucket.Name != values.Get(peerRESTListenBucket) {
			return false
		}
		return rulesMap.MatchSimple(ev.EventName, ev.S3.Object.Key)
	})

	for _, peer := range peers {
		if peer == nil {
			continue
		}
		peer.Listen(listenCh, ctx.Done(), values)
	}

	keepAliveTicker := time.NewTicker(500 * time.Millisecond)
	defer keepAliveTicker.Stop()

	enc := json.NewEncoder(w)
	for {
		select {
		case evI := <-listenCh:
			ev := evI.(event.Event)
			if len(string(ev.EventName)) > 0 {
				if err := enc.Encode(struct{ Records []event.Event }{[]event.Event{ev}}); err != nil {
					return
				}
			} else {
				if _, err := w.Write([]byte(" ")); err != nil {
					return
				}
			}
			w.(http.Flusher).Flush()
		case <-keepAliveTicker.C:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		case <-ctx.Done():
			return
		}
	}

}
