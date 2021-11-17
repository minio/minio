// Copyright (c) 2015-2021 MinIO, Inc.
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

package dns

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
)

var defaultWebhookContextTimeout = 10 * time.Second

func (c *WebhookDNS) addAuthHeader(r *http.Request) error {
	if c.username == "" || c.password == "" {
		return nil
	}

	claims := &jwt.StandardClaims{
		ExpiresAt: int64(15 * time.Minute),
		Issuer:    c.username,
		Subject:   config.EnvDNSWebhook,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	ss, err := token.SignedString([]byte(c.password))
	if err != nil {
		return err
	}

	r.Header.Set("Authorization", "Bearer "+ss)
	return nil
}

func (c *WebhookDNS) endpoint(bucket string, delete bool) (string, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if bucket != "" {
		q.Add("bucket", bucket)
	}
	if delete {
		q.Add("delete", strconv.FormatBool(delete))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Put - Adds DNS entries into webhook webhook server
func (c *WebhookDNS) Put(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWebhookContextTimeout)
	defer cancel()
	ep, err := c.endpoint(bucket, false)
	if err != nil {
		return newError(bucket, err)
	}
	var srvRecords []SrvRecord
	for _, domainName := range c.domainNames {
		srvRecords = append(srvRecords, SrvRecord{
			Host:         domainName,
			CreationDate: time.Now(),
		})
	}
	buf, err := json.Marshal(srvRecords)
	if err != nil {
		return newError(bucket, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, bytes.NewReader(buf))
	if err != nil {
		return newError(bucket, err)
	}
	if err = c.addAuthHeader(req); err != nil {
		return newError(bucket, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if derr := c.Delete(bucket); derr != nil {
			return newError(bucket, derr)
		}
		return err
	}
	defer xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var errorStringBuilder strings.Builder
		io.Copy(&errorStringBuilder, io.LimitReader(resp.Body, resp.ContentLength))
		errorString := errorStringBuilder.String()
		switch resp.StatusCode {
		case http.StatusConflict:
			return ErrBucketConflict(Error{bucket, errors.New(errorString)})
		}
		return newError(bucket, fmt.Errorf("service create for bucket %s, failed with status %s, error %s", bucket, resp.Status, errorString))
	}
	return nil
}

func newError(bucket string, err error) error {
	e := Error{bucket, err}
	if strings.Contains(err.Error(), "invalid bucket name") {
		return ErrInvalidBucketName(e)
	}
	return e
}

// Delete - Removes DNS entries added in Put().
func (c *WebhookDNS) Delete(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWebhookContextTimeout)
	defer cancel()
	ep, err := c.endpoint(bucket, true)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, nil)
	if err != nil {
		return err
	}
	if err = c.addAuthHeader(req); err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	xhttp.DrainBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusNotFound:
			return ErrNoEntriesFound
		}
		return fmt.Errorf("request to delete the service for bucket %s, failed with status %s", bucket, resp.Status)
	}
	return nil
}

// Close closes the internal http client
func (c *WebhookDNS) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// List - Retrieves list of DNS entries for the domain.
func (c *WebhookDNS) List() (srvRecords map[string][]SrvRecord, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWebhookContextTimeout)
	defer cancel()
	ep, err := c.endpoint("", false)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep, nil)
	if err != nil {
		return nil, err
	}
	if err = c.addAuthHeader(req); err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		// This is needed because our operator webhook
		// does not implement these methods.
		return nil, ErrNoEntriesFound
	}

	d := json.NewDecoder(resp.Body)
	if err = d.Decode(&srvRecords); err != nil {
		return nil, err
	}

	return srvRecords, nil
}

// Get - Retrieves DNS records for a bucket.
func (c *WebhookDNS) Get(bucket string) (srvRecords []SrvRecord, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultWebhookContextTimeout)
	defer cancel()
	ep, err := c.endpoint(bucket, false)
	if err != nil {
		return nil, newError(bucket, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep, nil)
	if err != nil {
		return nil, newError(bucket, err)
	}
	if err = c.addAuthHeader(req); err != nil {
		return nil, newError(bucket, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, newError(bucket, err)
	}
	defer xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusNotFound:
			// This is needed because our operator webhook
			// does not implement these methods and we need
			// a fallback.
			return nil, ErrNoEntriesFound
		default:
			return nil, newError(bucket, errors.New(resp.Status))
		}
	}

	d := json.NewDecoder(resp.Body)
	if err = d.Decode(&srvRecords); err != nil {
		return nil, newError(bucket, err)
	}
	if len(srvRecords) == 0 {
		return nil, ErrNoEntriesFound
	}
	return srvRecords, nil
}

// String stringer name for this implementation of dns.Store
func (c *WebhookDNS) String() string {
	return "webhookDNS"
}

// WebhookDNS - represents dns config for MinIO k8s webhook.
type WebhookDNS struct {
	httpClient  *http.Client
	Endpoint    string
	domainNames []string
	rootCAs     *x509.CertPool
	username    string
	password    string
}

// WebhookOption - functional options pattern style for WebhookDNS
type WebhookOption func(*WebhookDNS)

// PublicDomainNames set a list of domain names used by this WebhookDNS
// client setting, note this will fail if set to empty when
// constructor initializes.
func PublicDomainNames(domainNames ...string) WebhookOption {
	return func(args *WebhookDNS) {
		args.domainNames = domainNames
	}
}

// Authentication - custom username and password for authenticating at the endpoint
func Authentication(username, password string) WebhookOption {
	return func(args *WebhookDNS) {
		args.username = username
		args.password = password
	}
}

// RootCAs - add custom trust certs pool
func RootCAs(certPool *x509.CertPool) WebhookOption {
	return func(args *WebhookDNS) {
		args.rootCAs = certPool
	}
}

// NewWebhookDNS - initialize a new K8S Webhook DNS set/unset values.
func NewWebhookDNS(endpoint string, setters ...WebhookOption) (Store, error) {
	if endpoint == "" {
		return nil, errors.New("invalid argument")
	}

	args := &WebhookDNS{
		Endpoint: endpoint,
	}
	for _, setter := range setters {
		setter(args)
	}

	if len(args.domainNames) == 0 {
		return nil, errors.New("invalid argument")
	}

	args.httpClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 5 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: 3 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 3 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: args.rootCAs,
			},
			// Go net/http automatically unzip if content-type is
			// gzip disable this feature, as we are always interested
			// in raw stream.
			DisableCompression: true,
		},
	}
	return args, nil
}
