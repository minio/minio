/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/errors"
)

const (
	bucketConfigPrefix       = "buckets"
	bucketNotificationConfig = "notification.xml"
	bucketListenerConfig     = "listener.json"
)

// GetBucketNotificationHandler - This implementation of the GET
// operation uses the notification subresource to return the
// notification configuration of a bucket. If notifications are
// not enabled on the bucket, the operation returns an empty
// NotificationConfiguration element.
func (api objectAPIHandlers) GetBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {

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
	bucket := vars["bucket"]

	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Attempt to successfully load notification config.
	nConfig, err := loadNotificationConfig(bucket, objAPI)
	if err != nil && errors.Cause(err) != errNoSuchNotifications {
		errorIf(err, "Unable to read notification configuration.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	// For no notifications we write a dummy XML.
	if errors.Cause(err) == errNoSuchNotifications {
		// Complies with the s3 behavior in this regard.
		nConfig = &notificationConfig{}
	}
	notificationBytes, err := xml.Marshal(nConfig)
	if err != nil {
		// For any marshalling failure.
		errorIf(err, "Unable to marshal notification configuration into XML.", err)
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Success.
	writeSuccessResponseXML(w, notificationBytes)
}

// PutBucketNotificationHandler - Minio notification feature enables
// you to receive notifications when certain events happen in your bucket.
// Using this API, you can replace an existing notification configuration.
// The configuration is an XML file that defines the event types that you
// want Minio to publish and the destination where you want Minio to publish
// an event notification when it detects an event of the specified type.
// By default, your bucket has no event notifications configured. That is,
// the notification configuration will be an empty NotificationConfiguration.
func (api objectAPIHandlers) PutBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {

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
	bucket := vars["bucket"]

	_, err := objectAPI.GetBucketInfo(bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// If Content-Length is unknown or zero, deny the request.
	// PutBucketNotification always needs a Content-Length.
	if r.ContentLength == -1 || r.ContentLength == 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL)
		return
	}

	// Reads the incoming notification configuration.
	var buffer bytes.Buffer
	if r.ContentLength >= 0 {
		_, err = io.CopyN(&buffer, r.Body, r.ContentLength)
	} else {
		_, err = io.Copy(&buffer, r.Body)
	}
	if err != nil {
		errorIf(err, "Unable to read incoming body.")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	var notificationCfg notificationConfig
	// Unmarshal notification bytes.
	notificationConfigBytes := buffer.Bytes()
	if err = xml.Unmarshal(notificationConfigBytes, &notificationCfg); err != nil {
		errorIf(err, "Unable to parse notification configuration XML.")
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	} // Successfully marshalled notification configuration.

	// Validate unmarshalled bucket notification configuration.
	if s3Error := validateNotificationConfig(notificationCfg); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL)
		return
	}

	// Convert the incoming ARNs properly to the GetRegion().
	for i, queueConfig := range notificationCfg.QueueConfigs {
		queueConfig.QueueARN = unmarshalSqsARN(queueConfig.QueueARN).String()
		notificationCfg.QueueConfigs[i] = queueConfig
	}

	// Put bucket notification config.
	err = PutBucketNotificationConfig(bucket, &notificationCfg, objectAPI)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	// Success.
	writeSuccessResponseHeadersOnly(w)
}

// PutBucketNotificationConfig - Put a new notification config for a
// bucket (overwrites any previous config) persistently, updates
// global in-memory state, and notify other nodes in the cluster (if
// any)
func PutBucketNotificationConfig(bucket string, ncfg *notificationConfig, objAPI ObjectLayer) error {
	if ncfg == nil {
		return errInvalidArgument
	}

	// Acquire a write lock on bucket before modifying its
	// configuration.
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	// Release lock after notifying peers
	defer bucketLock.Unlock()

	// persist config to disk
	err := persistNotificationConfig(bucket, ncfg, objAPI)
	if err != nil {
		return fmt.Errorf("Unable to persist Bucket notification config to object layer - config=%v errMsg=%v", *ncfg, err)
	}

	// All servers (including local) are told to update in-memory config
	S3PeersUpdateBucketNotification(bucket, ncfg)

	return nil
}

// writeNotification marshals notification message before writing to client.
func writeNotification(w http.ResponseWriter, notification map[string][]NotificationEvent) error {
	// Invalid response writer.
	if w == nil {
		return errInvalidArgument
	}
	// Invalid notification input.
	if notification == nil {
		return errInvalidArgument
	}
	// Marshal notification data into JSON and write to client.
	notificationBytes, err := json.Marshal(&notification)
	if err != nil {
		return err
	}

	// Add additional CRLF characters for client to
	// differentiate the individual events properly.
	_, err = w.Write(append(notificationBytes, crlf...))
	// Make sure we have flushed, this would set Transfer-Encoding: chunked.
	w.(http.Flusher).Flush()
	return err
}

// CRLF character used for chunked transfer in accordance with HTTP standards.
var crlf = []byte("\r\n")

// listenChan A `listenChan` provides a data channel to send event
// notifications on and `doneCh` to signal that events are no longer
// being received. It also sends empty events (whitespace) to keep the
// underlying connection alive.
type listenChan struct {
	doneCh chan struct{}
	dataCh chan []NotificationEvent
}

// newListenChan returns a listenChan with properly initialized
// unbuffered channels.
func newListenChan() *listenChan {
	return &listenChan{
		doneCh: make(chan struct{}),
		dataCh: make(chan []NotificationEvent),
	}
}

// sendNotificationEvent sends notification events on the data channel
// unless doneCh is not closed
func (l *listenChan) sendNotificationEvent(events []NotificationEvent) {
	select {
	// Returns immediately if receiver has quit.
	case <-l.doneCh:
	// Blocks until receiver is available.
	case l.dataCh <- events:
	}
}

// waitForListener writes event notification OR whitespaces on
// ResponseWriter until client closes connection
func (l *listenChan) waitForListener(w http.ResponseWriter) {

	// Logs errors other than EPIPE and ECONNRESET.
	// EPIPE and ECONNRESET indicate that the client stopped
	// listening to notification events.
	logClientError := func(err error, msg string) {
		if oe, ok := err.(*net.OpError); ok && (oe.Err == syscall.EPIPE || oe.Err ==
			syscall.ECONNRESET) {
			errorIf(err, msg)
		}
	}

	emptyEvent := map[string][]NotificationEvent{"Records": nil}
	defer close(l.doneCh)
	for {
		select {
		case events := <-l.dataCh:
			if err := writeNotification(w, map[string][]NotificationEvent{"Records": events}); err != nil {
				logClientError(err, "Unable to write notification")
				return
			}
		case <-time.After(globalSNSConnAlive):
			if err := writeNotification(w, emptyEvent); err != nil {
				logClientError(err, "Unable to write empty notification")
				return
			}
		}
	}
}

// ListenBucketNotificationHandler - list bucket notifications.
func (api objectAPIHandlers) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
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
	bucket := vars["bucket"]

	// Parse listen bucket notification resources.
	prefixes, suffixes, events := getListenBucketNotificationResources(r.URL.Query())

	if err := validateFilterValues(prefixes); err != ErrNone {
		writeErrorResponse(w, err, r.URL)
		return
	}

	if err := validateFilterValues(suffixes); err != ErrNone {
		writeErrorResponse(w, err, r.URL)
		return
	}

	// Validate all the resource events.
	for _, event := range events {
		if errCode := checkEvent(event); errCode != ErrNone {
			writeErrorResponse(w, errCode, r.URL)
			return
		}
	}

	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}

	targetServer := GetLocalPeer(globalEndpoints)
	accountID := fmt.Sprintf("%d", UTCNow().UnixNano())
	accountARN := fmt.Sprintf(
		"%s:%s:%s:%s-%s",
		minioTopic,
		globalServerConfig.GetRegion(),
		accountID,
		snsTypeMinio,
		targetServer,
	)

	var filterRules []filterRule

	for _, prefix := range prefixes {
		filterRules = append(filterRules, filterRule{
			Name:  "prefix",
			Value: prefix,
		})
	}

	for _, suffix := range suffixes {
		filterRules = append(filterRules, filterRule{
			Name:  "suffix",
			Value: suffix,
		})
	}

	// Make topic configuration corresponding to this
	// ListenBucketNotification request.
	topicCfg := &topicConfig{
		TopicARN: accountARN,
		ServiceConfig: ServiceConfig{
			Events: events,
			Filter: struct {
				Key keyFilter `xml:"S3Key,omitempty" json:"S3Key,omitempty"`
			}{
				Key: keyFilter{
					FilterRules: filterRules,
				},
			},
			ID: "sns-" + accountID,
		},
	}

	// Setup a listen channel to receive notifications like
	// s3:ObjectCreated, s3:ObjectDeleted etc.
	nListenCh := newListenChan()
	// Add channel for listener events
	if err = globalEventNotifier.AddListenerChan(accountARN, nListenCh); err != nil {
		errorIf(err, "Error adding a listener!")
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	// Remove listener channel after the writer has closed or the
	// client disconnected.
	defer globalEventNotifier.RemoveListenerChan(accountARN)

	// Update topic config to bucket config and persist - as soon
	// as this call completes, events may start appearing in
	// nListenCh
	lc := listenerConfig{
		TopicConfig:  *topicCfg,
		TargetServer: targetServer,
	}

	err = AddBucketListenerConfig(bucket, &lc, objAPI)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(err), r.URL)
		return
	}
	defer RemoveBucketListenerConfig(bucket, &lc, objAPI)

	// Add all common headers.
	setCommonHeaders(w)

	// https://github.com/containous/traefik/issues/560
	// https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
	//
	// Proxies might buffer the connection to avoid this we
	// need the proper MIME type before writing to client.
	// This MIME header tells the proxies to avoid buffering
	w.Header().Set("Content-Type", "text/event-stream")

	// Start writing bucket notifications to ResponseWriter.
	nListenCh.waitForListener(w)
}

// AddBucketListenerConfig - Updates on disk state of listeners, and
// updates all peers with the change in listener config.
func AddBucketListenerConfig(bucket string, lcfg *listenerConfig, objAPI ObjectLayer) error {
	if lcfg == nil {
		return errInvalidArgument
	}
	listenerCfgs := globalEventNotifier.GetBucketListenerConfig(bucket)

	// add new lid to listeners and persist to object layer.
	listenerCfgs = append(listenerCfgs, *lcfg)

	// Acquire a write lock on bucket before modifying its
	// configuration.
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	// Release lock after notifying peers
	defer bucketLock.Unlock()

	// update persistent config if dist XL
	if globalIsDistXL {
		err := persistListenerConfig(bucket, listenerCfgs, objAPI)
		if err != nil {
			errorIf(err, "Error persisting listener config when adding a listener.")
			return err
		}
	}

	// persistence success - now update in-memory globals on all
	// peers (including local)
	S3PeersUpdateBucketListener(bucket, listenerCfgs)
	return nil
}

// RemoveBucketListenerConfig - removes a given bucket notification config
func RemoveBucketListenerConfig(bucket string, lcfg *listenerConfig, objAPI ObjectLayer) {
	listenerCfgs := globalEventNotifier.GetBucketListenerConfig(bucket)

	// remove listener with matching ARN - if not found ignore and exit.
	var updatedLcfgs []listenerConfig
	found := false
	for k, configuredLcfg := range listenerCfgs {
		if configuredLcfg.TopicConfig.TopicARN == lcfg.TopicConfig.TopicARN {
			updatedLcfgs = append(listenerCfgs[:k],
				listenerCfgs[k+1:]...)
			found = true
			break
		}
	}
	if !found {
		return
	}

	// Acquire a write lock on bucket before modifying its
	// configuration.
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if bucketLock.GetLock(globalOperationTimeout) != nil {
		return
	}
	// Release lock after notifying peers
	defer bucketLock.Unlock()

	// update persistent config if dist XL
	if globalIsDistXL {
		err := persistListenerConfig(bucket, updatedLcfgs, objAPI)
		if err != nil {
			errorIf(err, "Error persisting listener config when removing a listener.")
			return
		}
	}

	// persistence success - now update in-memory globals on all
	// peers (including local)
	S3PeersUpdateBucketListener(bucket, updatedLcfgs)
}
