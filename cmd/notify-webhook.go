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

// List of success status.
var successStatus = []int{
	http.StatusOK,
	http.StatusAccepted,
	http.StatusContinue,
	http.StatusNoContent,
	http.StatusPartialContent,
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
// a JSON which would send out minio release.
func lookupEndpoint(urlStr string) error {
	minioRelease := bytes.NewReader([]byte(ReleaseTag))
	req, err := http.NewRequest("POST", urlStr, minioRelease)
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
		Transport: &http.Transport{
			// need to close connection after usage.
			DisableKeepAlives: true,
		},
	}

	// Set content-type.
	req.Header.Set("Content-Type", "application/json")

	// Set proper server user-agent.
	req.Header.Set("User-Agent", globalServerUserAgent)

	// Retry if the request needs to be re-directed.
	// This code is necessary since Go 1.7.x do not
	// support retrying for http 307 for POST operation.
	// https://github.com/golang/go/issues/7912
	//
	// FIXME: Remove this when we move to Go 1.8.
	for {
		resp, derr := client.Do(req)
		if derr != nil {
			if isNetErrorIgnored(derr) {
				errorIf(derr, "Unable to lookup webhook endpoint %s", urlStr)
				return nil
			}
			return derr
		}
		if resp == nil {
			return fmt.Errorf("No response from server to download URL %s", urlStr)
		}
		resp.Body.Close()

		// Redo the request with the new redirect url if http 307
		// is returned, quit the loop otherwise
		if resp.StatusCode == http.StatusTemporaryRedirect {
			newURL, uerr := url.Parse(resp.Header.Get("Location"))
			if uerr != nil {
				return uerr
			}
			req.URL = newURL
			continue
		}

		// For any known successful http status, return quickly.
		for _, httpStatus := range successStatus {
			if httpStatus == resp.StatusCode {
				return nil
			}
		}

		err = fmt.Errorf("Unexpected response from webhook server %s: (%s)", urlStr, resp.Status)
		break
	}

	// Succes.
	return err
}

// Initializes new webhook logrus notifier.
func newWebhookNotify(accountID string) (*logrus.Logger, error) {
	rNotify := serverConfig.Notify.GetWebhookByID(accountID)
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
