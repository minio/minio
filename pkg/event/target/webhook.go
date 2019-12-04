/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package target

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// Webhook constants
const (
	WebhookEndpoint   = "endpoint"
	WebhookAuthToken  = "auth_token"
	WebhookQueueDir   = "queue_dir"
	WebhookQueueLimit = "queue_limit"

	EnvWebhookEnable     = "MINIO_NOTIFY_WEBHOOK_ENABLE"
	EnvWebhookEndpoint   = "MINIO_NOTIFY_WEBHOOK_ENDPOINT"
	EnvWebhookAuthToken  = "MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN"
	EnvWebhookQueueDir   = "MINIO_NOTIFY_WEBHOOK_QUEUE_DIR"
	EnvWebhookQueueLimit = "MINIO_NOTIFY_WEBHOOK_QUEUE_LIMIT"
)

// WebhookArgs - Webhook target arguments.
type WebhookArgs struct {
	Enable     bool           `json:"enable"`
	Endpoint   xnet.URL       `json:"endpoint"`
	AuthToken  string         `json:"authToken"`
	RootCAs    *x509.CertPool `json:"-"`
	QueueDir   string         `json:"queueDir"`
	QueueLimit uint64         `json:"queueLimit"`
}

// Validate WebhookArgs fields
func (w WebhookArgs) Validate() error {
	if !w.Enable {
		return nil
	}
	if w.Endpoint.IsEmpty() {
		return errors.New("endpoint empty")
	}
	if w.QueueDir != "" {
		if !filepath.IsAbs(w.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}
	if w.QueueLimit > maxLimit {
		return errors.New("queueLimit should not exceed 10000")
	}
	return nil
}

// WebhookTarget - Webhook target.
type WebhookTarget struct {
	id         event.TargetID
	args       WebhookArgs
	httpClient *http.Client
	store      Store
}

// ID - returns target ID.
func (target WebhookTarget) ID() event.TargetID {
	return target.id
}

// Save - saves the events to the store if queuestore is configured, which will be replayed when the wenhook connection is active.
func (target *WebhookTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	u, pErr := xnet.ParseHTTPURL(target.args.Endpoint.String())
	if pErr != nil {
		return pErr
	}
	if dErr := u.DialHTTP(); dErr != nil {
		if xnet.IsNetworkOrHostDown(dErr) {
			return errNotConnected
		}
		return dErr
	}
	return target.send(eventData)
}

// send - sends an event to the webhook.
func (target *WebhookTarget) send(eventData event.Event) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", target.args.Endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return err
	}

	if target.args.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+target.args.AuthToken)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := target.httpClient.Do(req)
	if err != nil {
		return err
	}

	// FIXME: log returned error. ignore time being.
	io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("sending event failed with %v", resp.Status)
	}

	return nil
}

// Send - reads an event from store and sends it to webhook.
func (target *WebhookTarget) Send(eventKey string) error {
	u, pErr := xnet.ParseHTTPURL(target.args.Endpoint.String())
	if pErr != nil {
		return pErr
	}
	if dErr := u.DialHTTP(); dErr != nil {
		if xnet.IsNetworkOrHostDown(dErr) {
			return errNotConnected
		}
		return dErr
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and would've been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	if err := target.send(eventData); err != nil {
		if xnet.IsNetworkOrHostDown(err) {
			return errNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - does nothing and available for interface compatibility.
func (target *WebhookTarget) Close() error {
	return nil
}

// NewWebhookTarget - creates new Webhook target.
func NewWebhookTarget(id string, args WebhookArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{})) (*WebhookTarget, error) {

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-webhook-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if err := store.Open(); err != nil {
			return nil, err
		}
	}

	target := &WebhookTarget{
		id:   event.TargetID{ID: id, Name: "webhook"},
		args: args,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: args.RootCAs},
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 5 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   3 * time.Second,
				ResponseHeaderTimeout: 3 * time.Second,
				ExpectContinueTimeout: 2 * time.Second,
			},
		},
		store: store,
	}

	if target.store != nil {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, loggerOnce)
	}

	return target, nil
}
