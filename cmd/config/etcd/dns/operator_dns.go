/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package dns

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/minio/minio/cmd/config"
)

var (
	defaultOperatorContextTimeout = 10 * time.Second
	// ErrNotImplemented - Indicates no entries were found for the given key (directory)
	ErrNotImplemented = errors.New("The method is not implemented")
	globalRootCAs     *x509.CertPool
)

// RegisterGlobalCAs register the global root CAs
func RegisterGlobalCAs(CAs *x509.CertPool) {
	globalRootCAs = CAs
}

func (c *OperatorDNS) addAuthHeader(r *http.Request) (*http.Request, error) {
	claims := &jwt.StandardClaims{
		ExpiresAt: int64(15 * time.Minute),
		Issuer:    c.Username,
		Subject:   config.EnvDNSWebhook,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	ss, err := token.SignedString([]byte(c.Password))
	if err != nil {
		return r, err
	}

	r.Header.Set("Authorization", "Bearer "+ss)
	return r, nil
}

func (c *OperatorDNS) endpoint(bucket string, delete bool) (string, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Add("bucket", bucket)
	if delete {
		q.Add("delete", "true")
	} else {
		q.Add("delete", "false")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Put - Adds DNS entries into operator webhook server
func (c *OperatorDNS) Put(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultOperatorContextTimeout)
	defer cancel()
	e, err := c.endpoint(bucket, false)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e, nil)
	if err != nil {
		return err
	}
	req, err = c.addAuthHeader(req)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if err := c.Delete(bucket); err != nil {
			return err
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request to create the service for bucket %s, failed with status %s", bucket, resp.Status)
	}
	return nil
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
	req, err = c.addAuthHeader(req)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
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

// OperatorDNS - represents dns config for MinIO k8s operator.
type OperatorDNS struct {
	httpClient *http.Client
	Endpoint   string
	Username   string
	Password   string
}

// NewOperatorDNS - initialize a new K8S Operator DNS set/unset values.
func NewOperatorDNS(endpoint, user, pwd string) (Store, error) {
	if endpoint == "" || user == "" || pwd == "" {
		return nil, errors.New("invalid argument")
	}
	args := &OperatorDNS{
		Username: user,
		Password: pwd,
		Endpoint: endpoint,
		httpClient: &http.Client{
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
					RootCAs: globalRootCAs,
				},
				// Go net/http automatically unzip if content-type is
				// gzip disable this feature, as we are always interested
				// in raw stream.
				DisableCompression: true,
			},
		},
	}

	return args, nil
}
