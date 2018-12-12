/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
)

const (
	bucketConfigPrefix       = "buckets"
	bucketNotificationConfig = "notification.xml"
	bucketListenerConfig     = "listener.json"
)

var errNoSuchNotifications = errors.New("The specified bucket does not have bucket notifications")

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
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	_, err := objAPI.GetBucketInfo(ctx, bucketName)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Attempt to successfully load notification config.
	nConfig, err := readNotificationConfig(ctx, objAPI, bucketName)
	if err != nil {
		// Ignore errNoSuchNotifications to comply with AWS S3.
		if err != errNoSuchNotifications {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		nConfig = &event.Config{}
	}

	// If xml namespace is empty, set a default value before returning.
	if nConfig.XMLNS == "" {
		nConfig.XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
	}

	notificationBytes, err := xml.Marshal(nConfig)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	writeSuccessResponseXML(w, notificationBytes)
}

// PutBucketNotificationHandler - This HTTP handler stores given notification configuration as per
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html.
func (api objectAPIHandlers) PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketNotification")

	defer logger.AuditLog(w, r, "PutBucketNotification", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if !objectAPI.IsNotificationSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	_, err := objectAPI.GetBucketInfo(ctx, bucketName)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// PutBucketNotification always needs a Content-Length.
	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
		return
	}

	var config *event.Config
	config, err = event.ParseConfig(io.LimitReader(r.Body, r.ContentLength), globalServerConfig.GetRegion(), globalNotificationSys.targetList)
	if err != nil {
		apiErr := ErrMalformedXML
		if event.IsEventError(err) {
			apiErr = toAPIErrorCode(ctx, err)
		}

		writeErrorResponse(w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	if err = saveNotificationConfig(ctx, objectAPI, bucketName, config); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rulesMap := config.ToRulesMap()
	globalNotificationSys.AddRulesMap(bucketName, rulesMap)
	globalNotificationSys.PutBucketNotification(ctx, bucketName, rulesMap)

	writeSuccessResponseHeadersOnly(w)
}

// ListenBucketNotificationHandler - This HTTP handler sends events to the connected HTTP client.
// Client should send prefix/suffix object name to match and events to watch as query parameters.
func (api objectAPIHandlers) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListenBucketNotification")

	defer logger.AuditLog(w, r, "ListenBucketNotification", mustGetClaimsFromToken(r))

	// Validate if bucket exists.
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}

	if !objAPI.IsListenBucketSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.ListenBucketNotificationAction, bucketName, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	values := r.URL.Query()

	var prefix string
	if len(values["prefix"]) > 1 {
		writeErrorResponse(w, ErrFilterNamePrefix, r.URL, guessIsBrowserReq(r))
	}
	if len(values["prefix"]) == 1 {
		if err := event.ValidateFilterRuleValue(values["prefix"][0]); err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		prefix = values["prefix"][0]
	}

	var suffix string
	if len(values["suffix"]) > 1 {
		writeErrorResponse(w, ErrFilterNameSuffix, r.URL, guessIsBrowserReq(r))
	}
	if len(values["suffix"]) == 1 {
		if err := event.ValidateFilterRuleValue(values["suffix"][0]); err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		suffix = values["suffix"][0]
	}

	pattern := event.NewPattern(prefix, suffix)

	eventNames := []event.Name{}
	for _, s := range values["events"] {
		eventName, err := event.ParseName(s)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		eventNames = append(eventNames, eventName)
	}

	if _, err := objAPI.GetBucketInfo(ctx, bucketName); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	host, err := xnet.ParseHost(r.RemoteAddr)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	target, err := target.NewHTTPClientTarget(*host, w)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	rulesMap := event.NewRulesMap(eventNames, pattern, target.ID())

	if err = globalNotificationSys.AddRemoteTarget(bucketName, target, rulesMap); err != nil {
		logger.GetReqInfo(ctx).AppendTags("target", target.ID().Name)
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	defer globalNotificationSys.RemoveRemoteTarget(bucketName, target.ID())
	defer globalNotificationSys.RemoveRulesMap(bucketName, rulesMap)

	thisAddr, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err = SaveListener(objAPI, bucketName, eventNames, pattern, target.ID(), *thisAddr); err != nil {
		logger.GetReqInfo(ctx).AppendTags("target", target.ID().Name)
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	globalNotificationSys.ListenBucketNotification(ctx, bucketName, eventNames, pattern, target.ID(), *thisAddr)

	<-target.DoneCh

	if err = RemoveListener(objAPI, bucketName, target.ID(), *thisAddr); err != nil {
		logger.GetReqInfo(ctx).AppendTags("target", target.ID().Name)
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
}
