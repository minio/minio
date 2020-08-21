/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	policy "github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
)

func (api objectAPIHandlers) ListenNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListenNotification")

	defer logger.AuditLog(w, r, "ListenNotification", mustGetClaimsFromToken(r))

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

	if !objAPI.IsListenSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	if bucketName == "" {
		if s3Error := checkRequestAuthType(ctx, r, policy.ListenNotificationAction, bucketName, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
			return
		}
	} else {
		if s3Error := checkRequestAuthType(ctx, r, policy.ListenBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	values := r.URL.Query()

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

	if bucketName != "" {
		if _, err := objAPI.GetBucketInfo(ctx, bucketName); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	setEventStreamHeaders(w)

	// Listen Publisher and peer-listen-client uses nonblocking send and hence does not wait for slow receivers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	listenCh := make(chan interface{}, 4000)

	peers := newPeerRestClients(globalEndpoints)

	globalHTTPListen.Subscribe(listenCh, ctx.Done(), func(evI interface{}) bool {
		ev, ok := evI.(event.Event)
		if !ok {
			return false
		}
		if ev.S3.Bucket.Name != "" && bucketName != "" {
			if ev.S3.Bucket.Name != bucketName {
				return false
			}
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
			ev, ok := evI.(event.Event)
			if ok {
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
