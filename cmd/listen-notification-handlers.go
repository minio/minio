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
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	policy "github.com/minio/pkg/bucket/policy"
)

func (api objectAPIHandlers) ListenNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListenNotification")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Validate if bucket exists.
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if !objAPI.IsListenSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	if bucketName == "" {
		if s3Error := checkRequestAuthType(ctx, r, policy.ListenNotificationAction, bucketName, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}
	} else {
		if s3Error := checkRequestAuthType(ctx, r, policy.ListenBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}
	}

	values := r.URL.Query()

	var prefix string
	if len(values[peerRESTListenPrefix]) > 1 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrFilterNamePrefix), r.URL)
		return
	}

	if len(values[peerRESTListenPrefix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenPrefix][0]); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		prefix = values[peerRESTListenPrefix][0]
	}

	var suffix string
	if len(values[peerRESTListenSuffix]) > 1 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrFilterNameSuffix), r.URL)
		return
	}

	if len(values[peerRESTListenSuffix]) == 1 {
		if err := event.ValidateFilterRuleValue(values[peerRESTListenSuffix][0]); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		suffix = values[peerRESTListenSuffix][0]
	}

	pattern := event.NewPattern(prefix, suffix)

	var eventNames []event.Name
	for _, s := range values[peerRESTListenEvents] {
		eventName, err := event.ParseName(s)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		eventNames = append(eventNames, eventName)
	}

	if bucketName != "" {
		if _, err := objAPI.GetBucketInfo(ctx, bucketName); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	setEventStreamHeaders(w)

	// Listen Publisher and peer-listen-client uses nonblocking send and hence does not wait for slow receivers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	listenCh := make(chan interface{}, 4000)

	peers, _ := newPeerRestClients(globalEndpoints)

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

	if bucketName != "" {
		values.Set(peerRESTListenBucket, bucketName)
	}
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
