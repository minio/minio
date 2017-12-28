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
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

type webhookNotify struct {
	Enable   bool   `json:"enable"`
	Endpoint string `json:"endpoint"`
}

func (w *webhookNotify) Validate() error {
	if !w.Enable {
		return nil
	}
	if _, err := checkURL(w.Endpoint); err != nil {
		return err
	}
	return nil
}

type httpConn struct {
	*http.Client
	Endpoint string
}

// isNetErrorIgnored - is network error ignored.
func isNetErrorIgnored(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "Client.Timeout exceeded while awaiting headers") {
		return true
	}
	switch err.(type) {
	case net.Error:
		switch e := err.(type) {
		case *net.DNSError, *net.OpError, net.UnknownNetworkError:
			return true
		case *url.Error:
			// Fixes https://github.com/minio/minio/issues/4050
			switch e.Err.(type) {
			case *net.DNSError, *net.OpError, net.UnknownNetworkError:
				return true
			}
			// For a URL error, where it replies back "connection closed"
			// retry again.
			if strings.Contains(err.Error(), "Connection closed by foreign host") {
				return true
			}
		default:
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") {
				// If error is - tlsHandshakeTimeoutError, retry.
				return true
			} else if strings.Contains(err.Error(), "i/o timeout") {
				// If error is - tcp timeoutError, retry.
				return true
			} else if strings.Contains(err.Error(), "connection timed out") {
				// If err is a net.Dial timeout, retry.
				return true
			}
		}
	}
	return false
}

// Lookup endpoint address by successfully POSTting
// empty body.
func lookupEndpoint(urlStr string) error {
	req, err := http.NewRequest("POST", urlStr, bytes.NewReader([]byte("")))
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
		Transport: &http.Transport{
			// Need to close connection after usage.
			DisableKeepAlives: true,
		},
	}

	// Set content-length to zero as there is no payload.
	req.ContentLength = 0

	// Set proper server user-agent.
	req.Header.Set("User-Agent", globalServerUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		if isNetErrorIgnored(err) {
			errorIf(err, "Unable to lookup webhook endpoint %s", urlStr)
			return nil
		}
		return err
	}
	defer resp.Body.Close()
	// HTTP status OK/NoContent.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Unable to lookup webhook endpoint %s response(%s)", urlStr, resp.Status)
	}
	return nil
}

// Initializes new webhook logrus notifier.
func newWebhookNotify(accountID string) (*logrus.Logger, error) {
	rNotify := globalServerConfig.Notify.GetWebhookByID(accountID)
	if rNotify.Endpoint == "" {
		return nil, errInvalidArgument
	}

	if err := lookupEndpoint(rNotify.Endpoint); err != nil {
		return nil, err
	}

	conn := httpConn{
		// Configure aggressive timeouts for client posts.
		Client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: globalRootCAs},
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
