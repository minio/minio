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
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
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
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// Validate request authorization.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	// Attempt to successfully load notification config.
	nConfig, err := loadNotificationConfig(bucket, objAPI)
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
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// Validate request authorization.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	_, err := objectAPI.GetBucketInfo(bucket)
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
	err = saveNotificationConfigFromBytes(bucket, objectAPI, bufferSize, bytes.NewReader(buffer.Bytes()))
	if err != nil {
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Set bucket notification config.
	globalEventNotifier.SetBucketNotificationConfig(bucket, &notificationCfg)

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
func sendBucketNotification(w http.ResponseWriter, arnListenerCh <-chan []NotificationEvent, peerListenerCh <-chan []byte) {
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
		case peerEvent := <-peerListenerCh:
			// write the event without any more processing
			// to response writer.
			_, err := w.Write(append(peerEvent, crlf...))
			w.(http.Flusher).Flush()
			if err != nil {
				errorIf(err, "Unable to write Peer notification to client.")
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
	// Validate if bucket exists.
	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(w, r, ErrServerNotInitialized, r.URL.Path)
		return
	}

	// Validate request authorization.
	if s3Error := checkAuth(r); s3Error != ErrNone {
		writeErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Parse listen bucket notification resources.
	prefix, suffix, events := getListenBucketNotificationResources(r.URL.Query())
	if !IsValidObjectPrefix(prefix) || !IsValidObjectPrefix(suffix) {
		writeErrorResponse(w, r, ErrFilterValueInvalid, r.URL.Path)
		return
	}

	// Validate all the resource events.
	for _, event := range events {
		if errCode := checkEvent(event); errCode != ErrNone {
			writeErrorResponse(w, r, errCode, r.URL.Path)
			return
		}
	}

	_, err := objAPI.GetBucketInfo(bucket)
	if err != nil {
		errorIf(err, "Unable to get bucket info.")
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	accountID := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	accountARN := "arn:minio:sns:" + serverConfig.GetRegion() + accountID + ":listen"
	var filterRules []filterRule
	if prefix != "" {
		filterRules = append(filterRules, filterRule{
			Name:  "prefix",
			Value: prefix,
		})
	}
	if suffix != "" {
		filterRules = append(filterRules, filterRule{
			Name:  "suffix",
			Value: suffix,
		})
	}

	// Make topic configuration corresponding to this ListenBucketNotification request.
	topicCfg := &topicConfig{
		TopicARN: accountARN,
		serviceConfig: serviceConfig{
			Events: events,
			Filter: struct {
				Key keyFilter `xml:"S3Key,omitempty"`
			}{
				Key: keyFilter{
					FilterRules: filterRules,
				},
			},
			ID: "sns-" + accountID,
		},
	}

	// Add topic config to bucket notification config and persists
	// config to disk.
	if err = globalEventNotifier.AddTopicConfig(bucket, topicCfg, objAPI); err != nil {
		writeErrorResponse(w, r, toAPIErrorCode(err), r.URL.Path)
		return
	}

	// Setup a listening channel that will receive notifications
	// from the RPC handler.
	nEventCh := make(chan []NotificationEvent)
	defer close(nEventCh)

	// Configuration is persisted to disk, we tell all
	// servers to send notifications for this topic to us.

	// Add all common headers.
	setCommonHeaders(w)

	// Set sns target.
	globalEventNotifier.SetSNSTarget(accountARN, nEventCh)
	// Remove sns listener after the writer has closed or the client disconnected.
	defer globalEventNotifier.RemoveSNSTarget(accountARN, nEventCh)

	// connect to each minio peer server to receive their
	// notifications.
	peerCh := make(chan []byte)
	// if r.Header.Get("X-Minio-Peer") == "" {
	// 	// Current request was not from a peer as the header
	// 	// is not set - so connect to all other peers to
	// 	// receive their notifications.
	// 	peers, err := getAllOtherPeers()
	// 	errorIf(err, "Error getting peer addrs - this shouldn't happen! Ploughing ahead anyway...")
	// 	for _, peer := range peers {
	// 		go listenNotificationFromPeer(peer, bucket, topicARN,
	// 			peerCh)
	// 	}
	// }

	// Start sending bucket notifications.
	sendBucketNotification(w, nEventCh, peerCh)
}

func listenNotificationFromPeer(peer, bucket, topicARN string, eventCh chan<- []byte) {
	httpClient := &http.Client{
		Transport: http.DefaultTransport,
	}
	for {
		// make http request to peer
		req, err := makeListenRequest(peer, bucket, topicARN)
		if err != nil {
			// TODO: handle this
			errorIf(err, "Error making a listen request to a peer")
			return
		}

		// execute request
		resp, err := httpClient.Do(req)
		if err != nil {
			// TODO: maybe a
			// failed server -
			// handle this.
			errorIf(err, "Error exec'ing a listen request to a peer")
			return
		}

		// TODO: check response status code
		if resp.StatusCode != http.StatusOK {
			// TODO: Another failure to handle
			errorIf(err, "Got non-StatusOK response in listen request to peer")
			return
		}

		// Initialize a scanner to read line by line
		bio := bufio.NewScanner(resp.Body)
		defer resp.Body.Close()

		for bio.Scan() {
			b := bio.Bytes()
			eventCh <- b
		}
		// Check for errors during scan
		if err = bio.Err(); err != nil {
			// For an unxpected connection drop from
			// server, we re-connect
			if err == io.ErrUnexpectedEOF {
				resp.Body.Close()
				continue
			}
			// otherwise log and exit
			errorIf(err, "Error reading peer server response")
			return
		}
	}
}

func makeListenRequest(host, bucket, topicARN string) (*http.Request, error) {
	method := "GET"
	payload := []byte{}
	cred := serverConfig.GetCredential()
	region := serverConfig.GetRegion()

	// TODO: need to check if connections have to be secure, etc
	url, err := url.Parse(fmt.Sprintf(
		"http://%s/%s?notificationARN=%s", host, bucket, topicARN,
	))
	if err != nil {
		return nil, err
	}

	// Initialize HTTP request
	req, err := http.NewRequest(method, url.String(), nil)
	if err != nil {
		return nil, err
	}

	// Add payload hash header (payload is empty)
	hash := hex.EncodeToString(sum256(payload))
	req.Header.Set("X-Amz-Content-Sha256", hash)

	// Add date header
	t := time.Now().UTC()
	req.Header.Set("X-Amz-Date", t.Format(iso8601Format))

	// Add X-Minio-Peer header
	req.Header.Set("X-Minio-Peer", host)

	// Compute signature V4
	queryStr := req.URL.Query().Encode()
	canonicalRequest := getCanonicalRequest(req.Header, hash, queryStr,
		req.URL.Path, req.Method, req.Host)
	stringToSign := getStringToSign(canonicalRequest, t, region)
	signingKey := getSigningKey(cred.SecretAccessKey, t, region)
	signature := getSignature(signingKey, stringToSign)

	// Compute Auth Header
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date",
		"x-minio-peer"} // sorted
	credentialHeader := strings.Join([]string{cred.AccessKeyID,
		t.Format(yyyymmdd), region, "s3", "aws4_request"}, "/")
	authHeaderValue := fmt.Sprintf(
		"%s Credential=%s/%s,SignedHeaders=%s,Signature=%s",
		signV4Algorithm, cred.AccessKeyID, credentialHeader,
		strings.Join(signedHeaders, ";"), signature,
	)

	// Set Auth Header
	req.Header.Set("Authorization", authHeaderValue)

	return req, nil
}

// returns the addresses of all servers in the cluster other than
// itself. Returned addresses are like "10.0.0.1:9000" (i.e. with
// port)
func getAllOtherPeers() ([]string, error) {
	res := []string{}
	port := getPort(srvConfig.serverAddr)
	diskPaths := srvConfig.disks
	for _, diskPath := range diskPaths {
		netAddr, _, err := splitNetPath(diskPath)
		if err != nil {
			return nil, err
		}
		// check if the host is the same as local
		if isLocalStorage(netAddr) {
			continue
		}
		// append port
		addrWithPort := fmt.Sprintf("%s:%d", netAddr, port)
		res = append(res, addrWithPort)
	}
	return res, nil
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
