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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
)

var (
	defaultOperatorContextTimeout = 10 * time.Second
	// ErrNotImplemented - Indicates the functionality which is not implemented
	ErrNotImplemented = errors.New("The method is not implemented")
)

func (c *OperatorDNS) addAuthHeader(r *http.Request) error {
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

func (c *OperatorDNS) endpoint(bucket string, delete bool) (string, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Add("bucket", bucket)
	q.Add("delete", strconv.FormatBool(delete))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Put - Adds DNS entries into operator webhook server
func (c *OperatorDNS) Put(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultOperatorContextTimeout)
	defer cancel()
	e, err := c.endpoint(bucket, false)
	if err != nil {
		return newError(bucket, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e, nil)
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
	}
	var errorStringBuilder strings.Builder
	io.Copy(&errorStringBuilder, io.LimitReader(resp.Body, resp.ContentLength))
	xhttp.DrainBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
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
func (c *OperatorDNS) Delete(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultOperatorContextTimeout)
	defer cancel()
	e, err := c.endpoint(bucket, true)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e, nil)
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
		return fmt.Errorf("request to delete the service for bucket %s, failed with status %s", bucket, resp.Status)
	}
	return nil
}

// DeleteRecord - Removes a specific DNS entry
// No Op for Operator because operator deals on with bucket entries
func (c *OperatorDNS) DeleteRecord(record SrvRecord) error {
	return ErrNotImplemented
}

// Close closes the internal http client
func (c *OperatorDNS) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// List - Retrieves list of DNS entries for the domain.
// This is a No Op for Operator because, there is no intent to enforce global
// namespace at MinIO level with this DNS entry. The global namespace in
// enforced by the Kubernetes Operator
func (c *OperatorDNS) List() (srvRecords map[string][]SrvRecord, err error) {
	return nil, ErrNotImplemented
}

// Get - Retrieves DNS records for a bucket.
// This is a No Op for Operator because, there is no intent to enforce global
// namespace at MinIO level with this DNS entry. The global namespace in
// enforced by the Kubernetes Operator
func (c *OperatorDNS) Get(bucket string) (srvRecords []SrvRecord, err error) {
	return nil, ErrNotImplemented
}

// String stringer name for this implementation of dns.Store
func (c *OperatorDNS) String() string {
	return "webhookDNS"
}

// OperatorDNS - represents dns config for MinIO k8s operator.
type OperatorDNS struct {
	httpClient *http.Client
	Endpoint   string
	rootCAs    *x509.CertPool
	username   string
	password   string
}

// OperatorOption - functional options pattern style for OperatorDNS
type OperatorOption func(*OperatorDNS)

// Authentication - custom username and password for authenticating at the endpoint
func Authentication(username, password string) OperatorOption {
	return func(args *OperatorDNS) {
		args.username = username
		args.password = password
	}
}

// RootCAs - add custom trust certs pool
func RootCAs(CAs *x509.CertPool) OperatorOption {
	return func(args *OperatorDNS) {
		args.rootCAs = CAs
	}
}

// NewOperatorDNS - initialize a new K8S Operator DNS set/unset values.
func NewOperatorDNS(endpoint string, setters ...OperatorOption) (Store, error) {
	if endpoint == "" {
		return nil, errors.New("invalid argument")
	}

	args := &OperatorDNS{
		Endpoint: endpoint,
	}
	for _, setter := range setters {
		setter(args)
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
