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
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Sirupsen/logrus"
)

type webhookNotify struct {
	Enable   bool   `json:"enable"`
	Endpoint string `json:"endpoint"`
}

type httpConn struct {
	*http.Client
	Endpoint string
}

// Lookup endpoint address by successfully dialing.
func lookupEndpoint(u *url.URL) error {
	dialer := &net.Dialer{
		Timeout:   300 * time.Millisecond,
		KeepAlive: 300 * time.Millisecond,
	}
	nconn, err := dialer.Dial("tcp", canonicalAddr(u))
	if err != nil {
		return err
	}
	return nconn.Close()
}

// Initializes new webhook logrus notifier.
func newWebhookNotify(accountID string) (*logrus.Logger, error) {
	rNotify := serverConfig.GetWebhookNotifyByID(accountID)

	if rNotify.Endpoint == "" {
		return nil, errInvalidArgument
	}

	u, err := url.Parse(rNotify.Endpoint)
	if err != nil {
		return nil, err
	}

	if err = lookupEndpoint(u); err != nil {
		return nil, err
	}

	conn := httpConn{
		// Configure aggressive timeouts for client posts.
		Client: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 5 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   3 * time.Second,
				ResponseHeaderTimeout: 3 * time.Second,
				ExpectContinueTimeout: 2 * time.Second,
			},
		},
		Endpoint: rNotify.Endpoint,
	}

	notifyLog := logrus.New()
	notifyLog.Out = ioutil.Discard

	// Set default JSON formatter.
	notifyLog.Formatter = new(logrus.JSONFormatter)

	notifyLog.Hooks.Add(conn)

	// Success
	return notifyLog, nil
}

// Fire is called when an event should be sent to the message broker.
func (n httpConn) Fire(entry *logrus.Entry) error {
	body, err := entry.Reader()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", n.Endpoint, body)
	if err != nil {
		return err
	}

	// Set content-type.
	req.Header.Set("Content-Type", "application/json")

	// Set proper server user-agent.
	req.Header.Set("User-Agent", globalServerUserAgent)

	// Initiate the http request.
	resp, err := n.Do(req)
	if err != nil {
		return err
	}

	// Make sure to close the response body so the connection can be re-used.
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusAccepted &&
		resp.StatusCode != http.StatusContinue {
		return fmt.Errorf("Unable to send event %s", resp.Status)
	}

	return nil
}

// Levels are Required for logrus hook implementation
func (httpConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
