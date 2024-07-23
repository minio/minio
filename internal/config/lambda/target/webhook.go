// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package target

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio/internal/config/lambda/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/certs"
	xnet "github.com/minio/pkg/v3/net"
)

// Webhook constants
const (
	WebhookEndpoint   = "endpoint"
	WebhookAuthToken  = "auth_token"
	WebhookClientCert = "client_cert"
	WebhookClientKey  = "client_key"

	EnvWebhookEnable     = "MINIO_LAMBDA_WEBHOOK_ENABLE"
	EnvWebhookEndpoint   = "MINIO_LAMBDA_WEBHOOK_ENDPOINT"
	EnvWebhookAuthToken  = "MINIO_LAMBDA_WEBHOOK_AUTH_TOKEN"
	EnvWebhookClientCert = "MINIO_LAMBDA_WEBHOOK_CLIENT_CERT"
	EnvWebhookClientKey  = "MINIO_LAMBDA_WEBHOOK_CLIENT_KEY"
)

// WebhookArgs - Webhook target arguments.
type WebhookArgs struct {
	Enable     bool            `json:"enable"`
	Endpoint   xnet.URL        `json:"endpoint"`
	AuthToken  string          `json:"authToken"`
	Transport  *http.Transport `json:"-"`
	ClientCert string          `json:"clientCert"`
	ClientKey  string          `json:"clientKey"`
}

// Validate WebhookArgs fields
func (w WebhookArgs) Validate() error {
	if !w.Enable {
		return nil
	}
	if w.Endpoint.IsEmpty() {
		return errors.New("endpoint empty")
	}
	if w.ClientCert != "" && w.ClientKey == "" || w.ClientCert == "" && w.ClientKey != "" {
		return errors.New("cert and key must be specified as a pair")
	}
	return nil
}

// WebhookTarget - Webhook target.
type WebhookTarget struct {
	activeRequests int64
	totalRequests  int64
	failedRequests int64

	lazyInit lazyInit

	id         event.TargetID
	args       WebhookArgs
	transport  *http.Transport
	httpClient *http.Client
	loggerOnce logger.LogOnce
	cancel     context.CancelFunc
	cancelCh   <-chan struct{}
}

// ID - returns target ID.
func (target *WebhookTarget) ID() event.TargetID {
	return target.id
}

// IsActive - Return true if target is up and active
func (target *WebhookTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

// errNotConnected - indicates that the target connection is not active.
var errNotConnected = errors.New("not connected to target server/service")

func (target *WebhookTarget) isActive() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target.args.Endpoint.String(), nil)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, false) {
			return false, errNotConnected
		}
		return false, err
	}
	tokens := strings.Fields(target.args.AuthToken)
	switch len(tokens) {
	case 2:
		req.Header.Set("Authorization", target.args.AuthToken)
	case 1:
		req.Header.Set("Authorization", "Bearer "+target.args.AuthToken)
	}

	resp, err := target.httpClient.Do(req)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, true) {
			return false, errNotConnected
		}
		return false, err
	}
	xhttp.DrainBody(resp.Body)
	// No network failure i.e response from the target means its up
	return true, nil
}

// Stat - returns lambda webhook target statistics such as
// current calls in progress, successfully completed functions
// failed functions.
func (target *WebhookTarget) Stat() event.TargetStat {
	return event.TargetStat{
		ID:             target.id,
		ActiveRequests: atomic.LoadInt64(&target.activeRequests),
		TotalRequests:  atomic.LoadInt64(&target.totalRequests),
		FailedRequests: atomic.LoadInt64(&target.failedRequests),
	}
}

// Send - sends an event to the webhook.
func (target *WebhookTarget) Send(eventData event.Event) (resp *http.Response, err error) {
	atomic.AddInt64(&target.activeRequests, 1)
	defer atomic.AddInt64(&target.activeRequests, -1)

	atomic.AddInt64(&target.totalRequests, 1)
	defer func() {
		if err != nil {
			atomic.AddInt64(&target.failedRequests, 1)
		}
	}()

	if err = target.init(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(eventData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, target.args.Endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// Verify if the authToken already contains
	// <Key> <Token> like format, if this is
	// already present we can blindly use the
	// authToken as is instead of adding 'Bearer'
	tokens := strings.Fields(target.args.AuthToken)
	switch len(tokens) {
	case 2:
		req.Header.Set("Authorization", target.args.AuthToken)
	case 1:
		req.Header.Set("Authorization", "Bearer "+target.args.AuthToken)
	}

	req.Header.Set("Content-Type", "application/json")

	return target.httpClient.Do(req)
}

// Close the target. Will cancel all active requests.
func (target *WebhookTarget) Close() error {
	target.cancel()
	return nil
}

func (target *WebhookTarget) init() error {
	return target.lazyInit.Do(target.initWebhook)
}

// Only called from init()
func (target *WebhookTarget) initWebhook() error {
	args := target.args
	transport := target.transport

	if args.ClientCert != "" && args.ClientKey != "" {
		manager, err := certs.NewManager(context.Background(), args.ClientCert, args.ClientKey, tls.LoadX509KeyPair)
		if err != nil {
			return err
		}
		manager.ReloadOnSignal(syscall.SIGHUP) // allow reloads upon SIGHUP
		transport.TLSClientConfig.GetClientCertificate = manager.GetClientCertificate
	}
	target.httpClient = &http.Client{Transport: transport}

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return errNotConnected
	}

	return nil
}

// NewWebhookTarget - creates new Webhook target.
func NewWebhookTarget(ctx context.Context, id string, args WebhookArgs, loggerOnce logger.LogOnce, transport *http.Transport) (*WebhookTarget, error) {
	ctx, cancel := context.WithCancel(ctx)

	target := &WebhookTarget{
		id:         event.TargetID{ID: id, Name: "webhook"},
		args:       args,
		loggerOnce: loggerOnce,
		transport:  transport,
		cancel:     cancel,
		cancelCh:   ctx.Done(),
	}

	return target, nil
}
