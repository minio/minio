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

package main

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"path"

	"github.com/gorilla/mux"
)

const (
	bucketConfigPrefix       = "buckets"
	bucketNotificationConfig = "notification.xml"
)

// loads notifcation config if any for a given bucket, returns back structured notification config.
func (api objectAPIHandlers) loadNotificationConfig(bucket string) (nConfig notificationConfig, err error) {
	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	var objInfo ObjectInfo
	objInfo, err = api.ObjectAPI.GetObjectInfo(minioMetaBucket, notificationConfigPath)
	if err != nil {
		switch err.(type) {
		case ObjectNotFound:
			return notificationConfig{}, nil
		}
		return notificationConfig{}, err
	}
	var buffer bytes.Buffer
	err = api.ObjectAPI.GetObject(minioMetaBucket, notificationConfigPath, 0, objInfo.Size, &buffer)
	if err != nil {
		switch err.(type) {
		case ObjectNotFound:
			return notificationConfig{}, nil
		}
		return notificationConfig{}, err
	}

	// Unmarshal notification bytes.
	notificationConfigBytes := buffer.Bytes()
	if err = xml.Unmarshal(notificationConfigBytes, &nConfig); err != nil {
		return notificationConfig{}, err
	} // Successfully marshalled notification configuration.

	return nConfig, nil
}

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
	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	objInfo, err := api.ObjectAPI.GetObjectInfo(minioMetaBucket, notificationConfigPath)
	if err != nil {
		switch err.(type) {
		case ObjectNotFound:
			writeSuccessResponse(w, nil)
			return
		}
		errorIf(err, "Unable to read notification configuration.", notificationConfigPath)
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Indicates if any data was written to the http.ResponseWriter
	dataWritten := false

	// io.Writer type which keeps track if any data was written.
	writer := funcToWriter(func(p []byte) (int, error) {
		if !dataWritten {
			// Set headers on the first write.
			// Set standard object headers.
			setObjectHeaders(w, objInfo, nil)

			// Set any additional requested response headers.
			setGetRespHeaders(w, r.URL.Query())

			dataWritten = true
		}
		return w.Write(p)
	})

	// Reads the object at startOffset and writes to func writer..
	err = api.ObjectAPI.GetObject(minioMetaBucket, notificationConfigPath, 0, objInfo.Size, writer)
	if err != nil {
		if !dataWritten {
			switch err.(type) {
			case ObjectNotFound:
				writeSuccessResponse(w, nil)
				return
			}
			// Error response only if no data has been written to client yet. i.e if
			// partial data has already been written before an error
			// occurred then no point in setting StatusCode and
			// sending error XML.
			apiErr := toAPIErrorCode(err)
			writeErrorResponse(w, r, apiErr, r.URL.Path)
		}
		errorIf(err, "Unable to write to client.")
		return
	}
	if !dataWritten {
		// If ObjectAPI.GetObject did not return error and no data has
		// been written it would mean that it is a 0-byte object.
		// call wrter.Write(nil) to set appropriate headers.
		writer.Write(nil)
	}

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
		errorIf(err, "Unable to bucket info.")
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
	if r.ContentLength >= 0 {
		_, err = io.CopyN(&buffer, r.Body, r.ContentLength)
	} else {
		_, err = io.Copy(&buffer, r.Body)
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
	size := int64(len(notificationConfigBytes))
	data := bytes.NewReader(notificationConfigBytes)
	notificationConfigPath := path.Join(bucketConfigPrefix, bucket, bucketNotificationConfig)
	_, err = api.ObjectAPI.PutObject(minioMetaBucket, notificationConfigPath, size, data, nil)
	if err != nil {
		errorIf(err, "Unable to write bucket notification configuration.", notificationConfigPath)
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Success.
	writeSuccessResponse(w, nil)
}
