// Copyright (c) 2015-2023 MinIO, Inc.
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
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/grid"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
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

	values := r.Form

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
	var mask pubsub.Mask
	for _, s := range values[peerRESTListenEvents] {
		eventName, err := event.ParseName(s)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		mask.MergeMaskable(eventName)
		eventNames = append(eventNames, eventName)
	}

	if bucketName != "" {
		if _, err := objAPI.GetBucketInfo(ctx, bucketName, BucketOptions{}); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, event.TargetID{ID: mustGetUUID()})

	setEventStreamHeaders(w)

	// Listen Publisher and peer-listen-client uses nonblocking send and hence does not wait for slow receivers.
	// Use buffered channel to take care of burst sends or slow w.Write()
	mergeCh := make(chan []byte, globalAPIConfig.getRequestsPoolCapacity()*len(globalEndpoints.Hostnames()))
	localCh := make(chan event.Event, globalAPIConfig.getRequestsPoolCapacity())

	// Convert local messages to JSON and send to mergeCh
	go func() {
		buf := bytes.NewBuffer(grid.GetByteBuffer()[:0])
		enc := json.NewEncoder(buf)
		tmpEvt := struct{ Records []event.Event }{[]event.Event{{}}}
		for {
			select {
			case ev := <-localCh:
				buf.Reset()
				tmpEvt.Records[0] = ev
				if err := enc.Encode(tmpEvt); err != nil {
					bugLogIf(ctx, err, "event: Encode failed")
					continue
				}
				mergeCh <- append(grid.GetByteBuffer()[:0], buf.Bytes()...)
			case <-ctx.Done():
				grid.PutByteBuffer(buf.Bytes())
				return
			}
		}
	}()
	peers, _ := newPeerRestClients(globalEndpoints)
	err := globalHTTPListen.Subscribe(mask, localCh, ctx.Done(), func(ev event.Event) bool {
		if ev.S3.Bucket.Name != "" && bucketName != "" {
			if ev.S3.Bucket.Name != bucketName {
				return false
			}
		}
		return rulesMap.MatchSimple(ev.EventName, ev.S3.Object.Key)
	})
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if bucketName != "" {
		values.Set(peerRESTListenBucket, bucketName)
	}
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		peer.Listen(ctx, mergeCh, values)
	}

	var (
		emptyEventTicker <-chan time.Time
		keepAliveTicker  <-chan time.Time
	)

	if p := values.Get("ping"); p != "" {
		pingInterval, err := strconv.Atoi(p)
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidQueryParams), r.URL)
			return
		}
		if pingInterval < 1 {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidQueryParams), r.URL)
			return
		}
		t := time.NewTicker(time.Duration(pingInterval) * time.Second)
		defer t.Stop()
		emptyEventTicker = t.C
	} else {
		// Deprecated Apr 2023
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		keepAliveTicker = t.C
	}

	enc := json.NewEncoder(w)
	for {
		select {
		case ev := <-mergeCh:
			_, err := w.Write(ev)
			if err != nil {
				return
			}
			if len(mergeCh) == 0 {
				// Flush if nothing is queued
				xhttp.Flush(w)
			}
			grid.PutByteBuffer(ev)
		case <-emptyEventTicker:
			if err := enc.Encode(struct{ Records []event.Event }{}); err != nil {
				return
			}
			xhttp.Flush(w)
		case <-keepAliveTicker:
			if _, err := w.Write([]byte(" ")); err != nil {
				return
			}
			xhttp.Flush(w)
		case <-ctx.Done():
			return
		}
	}
}
