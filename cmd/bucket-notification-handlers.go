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
	xerrors "github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/event/target"
	xnet "github.com/minio/minio/pkg/net"
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
	ctx := newContext(r, "GetBucketNotification")

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}
	if s3Error := checkRequestAuthType(r, "", "", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	_, err := objAPI.GetBucketInfo(ctx, bucketName)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Attempt to successfully load notification config.
	nConfig, err := readNotificationConfig(objAPI, bucketName)
	if err != nil {
		// Ignore errNoSuchNotifications to comply with AWS S3.
		if xerrors.Cause(err) != errNoSuchNotifications {
			errorIf(err, "Unable to read notification configuration.")
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}

		nConfig = &event.Config{}
	}

	notificationBytes, err := xml.Marshal(nConfig)
	if err != nil {
		errorIf(err, "Unable to marshal notification configuration into XML.", err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	writeSuccessResponseXML(w, notificationBytes)
}

// PutBucketNotificationHandler - This HTTP handler stores given notification configuration as per
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html.
func (api objectAPIHandlers) PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, "PutBucketNotification")

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}

	if !objectAPI.IsNotificationSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}
	if s3Error := checkRequestAuthType(r, "", "", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	_, err := objectAPI.GetBucketInfo(ctx, bucketName)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// PutBucketNotification always needs a Content-Length.
	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}

	var config *event.Config
	config, err = event.ParseConfig(io.LimitReader(r.Body, r.ContentLength), globalServerConfig.GetRegion(), globalNotificationSys.targetList)
	if err != nil {
		apiErr := ErrMalformedXML
		if event.IsEventError(err) {
			apiErr = toAPIErrorCode(err)
		}

		writeErrorResponse(w, apiErr, r.URL)
		return
	}

	if err = saveNotificationConfig(objectAPI, bucketName, config); err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	rulesMap := config.ToRulesMap()
	globalNotificationSys.AddRulesMap(bucketName, rulesMap)
	for addr, err := range globalNotificationSys.PutBucketNotification(bucketName, rulesMap) {
		errorIf(err, "unable to put bucket notification to remote peer %v", addr)
	}

	writeSuccessResponseHeadersOnly(w)
}

// ListenBucketNotificationHandler - This HTTP handler sends events to the connected HTTP client.
// Client should send prefix/suffix object name to match and events to watch as query parameters.
func (api objectAPIHandlers) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, "ListenBucketNotification")

	// Validate if bucket exists.
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL)
		return
	}
	if !objAPI.IsNotificationSupported() {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}
	if s3Error := checkRequestAuthType(r, "", "", globalServerConfig.GetRegion()); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	values := r.URL.Query()

	var prefix string
	if len(values["prefix"]) > 1 {
		writeErrorResponse(w, ErrFilterNamePrefix, r.URL)
	}
	if len(values["prefix"]) == 1 {
		if err := event.ValidateFilterRuleValue(values["prefix"][0]); err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}

		prefix = values["prefix"][0]
	}

	var suffix string
	if len(values["suffix"]) > 1 {
		writeErrorResponse(w, ErrFilterNameSuffix, r.URL)
	}
	if len(values["suffix"]) == 1 {
		if err := event.ValidateFilterRuleValue(values["suffix"][0]); err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}

		suffix = values["suffix"][0]
	}

	pattern := event.NewPattern(prefix, suffix)

	eventNames := []event.Name{}
	for _, s := range values["events"] {
		eventName, err := event.ParseName(s)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(err), r.URL)
			return
		}

		eventNames = append(eventNames, eventName)
	}

	if _, err := objAPI.GetBucketInfo(ctx, bucketName); err != nil {
		errorIf(err, "Unable to get bucket info.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	host := xnet.MustParseHost(r.RemoteAddr)
	target := target.NewHTTPClientTarget(*host, w)
	rulesMap := event.NewRulesMap(eventNames, pattern, target.ID())

	if err := globalNotificationSys.AddRemoteTarget(bucketName, target, rulesMap); err != nil {
		errorIf(err, "Unable to add httpclient target %v to globalNotificationSys.targetList.", target)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	defer globalNotificationSys.RemoveRemoteTarget(bucketName, target.ID())
	defer globalNotificationSys.RemoveRulesMap(bucketName, rulesMap)

	thisAddr := xnet.MustParseHost(GetLocalPeer(globalEndpoints))
	if err := SaveListener(objAPI, bucketName, eventNames, pattern, target.ID(), *thisAddr); err != nil {
		errorIf(err, "Unable to save HTTP listener %v", target)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	errors := globalNotificationSys.ListenBucketNotification(bucketName, eventNames, pattern, target.ID(), *thisAddr)
	for addr, err := range errors {
		errorIf(err, "unable to call listen bucket notification to remote peer %v", addr)
	}

	<-target.DoneCh

	if err := RemoveListener(objAPI, bucketName, target.ID(), *thisAddr); err != nil {
		errorIf(err, "Unable to save HTTP listener %v", target)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
}
