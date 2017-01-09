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
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"encoding/json"

	"github.com/Sirupsen/logrus"
)

type webhookNotify struct {
	Enable   bool   `json:"enable"`
	Endpoint string `json:"endpoint"`
}

type httpConn struct {
	HTTPClient *http.Client
	Endpoint   string
}

type httpPostBody struct {
	Records   interface{}
	Key       string
	EventType string
}

func newWebhookNotify(accountID string) (*logrus.Logger, error) {
	rNotify := serverConfig.GetWebhookNotifyByID(accountID)

	connection := httpConn{
		Endpoint: rNotify.Endpoint,
	}

	u, err := url.ParseRequestURI(rNotify.Endpoint)
	if err != nil {
		return nil, err
	}
	_, err = net.LookupHost(u.Host)
	if err != nil {
		return nil, err
	}

	// Configure aggressive timeouts for client posts.
	connection.HTTPClient = &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 5 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   3 * time.Second,
			ResponseHeaderTimeout: 3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	notifyLog := logrus.New()
	notifyLog.Out = ioutil.Discard

	// Set default JSON formatter.
	notifyLog.Formatter = new(logrus.JSONFormatter)

	notifyLog.Hooks.Add(connection)

	// Success
	return notifyLog, nil
}

// Fire is called when an event should be sent to the message broker.
func (n httpConn) Fire(entry *logrus.Entry) error {

	// Fetch event type upon reflecting on its original type.
	entryStr, ok := entry.Data["EventType"].(string)
	if !ok {
		return nil
	}

	httpPostBody1 := httpPostBody{
		Records:   entry.Data["Records"],
		Key:       entry.Data["Key"].(string),
		EventType: entryStr,
	}

	itemsStr, err := json.Marshal(httpPostBody1)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(itemsStr)

	// Enhancement: consider using "Request" so we can set the User-agent
	req, postErr := n.HTTPClient.Post(n.Endpoint, "application/json", buf)
	if postErr != nil {
		entry.Warn(postErr)
	}
	if req.StatusCode != http.StatusOK &&
		req.StatusCode != http.StatusAccepted &&
		req.StatusCode != http.StatusContinue {

		return errors.New("Got status code: " + strconv.Itoa(req.StatusCode))
	}

	return nil
}

// Levels are Required for logrus hook implementation
func (httpConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
