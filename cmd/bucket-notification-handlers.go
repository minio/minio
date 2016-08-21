/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"io"
	"net/http"
	"path"
	"time"

	"github.com/gorilla/mux"
)

const (
	bucketConfigPrefix       = "buckets"
	bucketNotificationConfig = "notification.xml"
)

// GetBucketNotificationHandler - This implementation of the GET
// operation uses the notification subresource to return the
// notification configuration of a bucket. If notifications are
// not enabled on the bucket, the operation returns an empty
// NotificationConfiguration element.
func (api objectAPIHandlers) GetBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	// Validate request authorization.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	// Attempt to successfully load notification config.
	nConfig, err := loadNotificationConfig(bucket, api.ObjectAPI)
	if err != nil && err != errNoSuchNotifications {
		errorIf(err, "Unable to read notification configuration.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	// For no notifications we write a dummy XML.
	if err == errNoSuchNotifications {
		// Complies with the s3 behavior in this regard.
		nConfig = &notificationConfig{}
	}
	notificationBytes, err := xml.Marshal(nConfig)
	if err != nil {
		// For any marshalling failure.
		errorIf(err, "Unable to marshal notification configuration into XML.", err)
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}
	// Success.
	writeSuccessResponse(w, notificationBytes)
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
	// Validate request authorization.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	_, err := api.ObjectAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to find bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// If Content-Length is unknown or zero, deny the request. PutBucketNotification
	// always needs a Content-Length if incoming request is not chunked.
	if !contains(r.TransferEncoding, "chunked") {
		if r.ContentLength == -1 {
			writeErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
			return
		}
	}

	// Reads the incoming notification configuration.
	var buffer bytes.Buffer
	var bufferSize int64
	if r.ContentLength >= 0 {
		bufferSize, err = io.CopyN(&buffer, r.Body, r.ContentLength)
	} else {
		bufferSize, err = io.Copy(&buffer, r.Body)
	}
	if err != nil {
		errorIf(err, "Unable to read incoming body.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	var notificationCfg notificationConfig
	// Unmarshal notification bytes.
	notificationConfigBytes := buffer.Bytes()
	if err = xml.Unmarshal(notificationConfigBytes, &notificationCfg); err != nil {
		errorIf(err, "Unable to parse notification configuration XML.")
		writeErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	} // Successfully marshalled notification configuration.

	// Validate unmarshalled bucket notification configuration.
	if s3Error := validateNotificationConfig(notificationCfg); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Proceed to save notification configuration.
	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	_, err = api.ObjectAPI.PutObject(minioMetaBucket, notificationConfigPath, bufferSize, bytes.NewReader(buffer.Bytes()), nil)
	if err != nil {
		errorIf(err, "Unable to write bucket notification configuration.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Set bucket notification config.
	eventN.SetBucketNotificationConfig(bucket, &notificationCfg)

	// Success.
	writeSuccessResponse(w, nil)
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
	// Marshal notification data into XML and write to client.
	notificationBytes, err := json.Marshal(&notification)
	if err != nil {
		return err
	}
	// Add additional CRLF characters for client to
	// differentiate the individual events properly.
	_, err = w.Write(append(notificationBytes, crlf...))
	// Make sure we have flushed, this would set Transfer-Encoding: chunked.
	w.(http.Flusher).Flush()
	if err != nil {
		return err
	}
	return nil
}

// CRLF character used for chunked transfer in accordance with HTTP standards.
var crlf = []byte("\r\n")

// sendBucketNotification - writes notification back to client on the response writer
// for each notification input, otherwise writes whitespace characters periodically
// to keep the connection active. Each notification messages are terminated by CRLF
// character. Upon any error received on response writer the for loop exits.
//
// TODO - do not log for all errors.
func sendBucketNotification(w http.ResponseWriter, arnListenerCh <-chan []NotificationEvent) {
	var dummyEvents = map[string][]NotificationEvent{"Records": nil}
	// Continuously write to client either timely empty structures
	// every 5 seconds, or return back the notifications.
	for {
		select {
		case events := <-arnListenerCh:
			if err := writeNotification(w, map[string][]NotificationEvent{"Records": events}); err != nil {
				errorIf(err, "Unable to write notification to client.")
				return
			}
		case <-time.After(5 * time.Second):
			if err := writeNotification(w, dummyEvents); err != nil {
				errorIf(err, "Unable to write notification to client.")
				return
			}
		}
	}
}

// ListenBucketNotificationHandler - list bucket notifications.
func (api objectAPIHandlers) ListenBucketNotificationHandler(w http.ResponseWriter, r *http.Request) {
	// Validate request authorization.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Get notification ARN.
	topicARN := r.URL.Query().Get("notificationARN")
	if topicARN == "" {
		writeErrorResponse(w, r, ErrARNNotification, r.URL.Path)
		return
	}

	// Validate if bucket exists.
	_, err := api.ObjectAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	notificationCfg := eventN.GetBucketNotificationConfig(bucket)
	if notificationCfg == nil {
		writeErrorResponse(w, r, ErrARNNotification, r.URL.Path)
		return
	}

	// Set SNS notifications only if special "listen" sns is set in bucket
	// notification configs.
	if !isMinioSNSConfigured(topicARN, notificationCfg.TopicConfigs) {
		writeErrorResponse(w, r, ErrARNNotification, r.URL.Path)
		return
	}

	// Add all common headers.
	setCommonHeaders(w)

	// Create a new notification event channel.
	nEventCh := make(chan []NotificationEvent)
	// Close the listener channel.
	defer close(nEventCh)

	// Set sns target.
	eventN.SetSNSTarget(topicARN, nEventCh)
	// Remove sns listener after the writer has closed or the client disconnected.
	defer eventN.RemoveSNSTarget(topicARN, nEventCh)

	// Start sending bucket notifications.
	sendBucketNotification(w, nEventCh)
}

// Removes notification.xml for a given bucket, only used during DeleteBucket.
func removeNotificationConfig(bucket string, objAPI ObjectLayer) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	return objAPI.DeleteObject(minioMetaBucket, notificationConfigPath)
}
