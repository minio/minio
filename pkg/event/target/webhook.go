/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// WebhookArgs - Webhook target arguments.
type WebhookArgs struct {
	Enable   bool           `json:"enable"`
	Endpoint xnet.URL       `json:"endpoint"`
	RootCAs  *x509.CertPool `json:"-"`
}

// WebhookTarget - Webhook target.
type WebhookTarget struct {
	id         event.TargetID
	args       WebhookArgs
	httpClient *http.Client
}

// ID - returns target ID.
func (target WebhookTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to Webhook.
func (target *WebhookTarget) Send(eventData event.Event) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{eventData.EventName, key, []event.Event{eventData}})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", target.args.Endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return err
	}

	// req.Header.Set("User-Agent", globalServerUserAgent)
	req.Header.Set("Content-Type", "application/json")

	resp, err := target.httpClient.Do(req)
	if err != nil {
		return err
	}

	// FIXME: log returned error. ignore time being.
	_ = resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("sending event failed with %v", resp.Status)
	}

	return nil
}

// Close - does nothing and available for interface compatibility.
func (target *WebhookTarget) Close() error {
	return nil
}

// NewWebhookTarget - creates new Webhook target.
func NewWebhookTarget(id string, args WebhookArgs) *WebhookTarget {
	return &WebhookTarget{
		id:   event.TargetID{id, "webhook"},
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
	}
}
