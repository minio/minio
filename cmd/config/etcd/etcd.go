/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package etcd

import (
	"crypto/tls"
	"crypto/x509"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	// Default values used while communicating with etcd.
	defaultDialTimeout   = 30 * time.Second
	defaultDialKeepAlive = 30 * time.Second
)

// etcd environment values
const (
	EnvEtcdEndpoints     = "MINIO_ETCD_ENDPOINTS"
	EnvEtcdClientCert    = "MINIO_ETCD_CLIENT_CERT"
	EnvEtcdClientCertKey = "MINIO_ETCD_CLIENT_CERT_KEY"
)

// New - Initialize new etcd client
func New(rootCAs *x509.CertPool) (*clientv3.Client, error) {
	envEndpoints := env.Get(EnvEtcdEndpoints, "")
	if envEndpoints == "" {
		// etcd is not configured, nothing to do.
		return nil, nil
	}

	etcdEndpoints := strings.Split(envEndpoints, config.ValueSeparator)

	var etcdSecure bool
	for _, endpoint := range etcdEndpoints {
		u, err := xnet.ParseURL(endpoint)
		if err != nil {
			return nil, err
		}
		// If one of the endpoint is https, we will use https directly.
		etcdSecure = etcdSecure || u.Scheme == "https"
	}

	var err error
	var etcdClnt *clientv3.Client
	if etcdSecure {
		// This is only to support client side certificate authentication
		// https://coreos.com/etcd/docs/latest/op-guide/security.html
		etcdClientCertFile, ok1 := env.Lookup(EnvEtcdClientCert)
		etcdClientCertKey, ok2 := env.Lookup(EnvEtcdClientCertKey)
		var getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error)
		if ok1 && ok2 {
			getClientCertificate = func(unused *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, terr := tls.LoadX509KeyPair(etcdClientCertFile, etcdClientCertKey)
				return &cert, terr
			}
		}

		etcdClnt, err = clientv3.New(clientv3.Config{
			Endpoints:         etcdEndpoints,
			DialTimeout:       defaultDialTimeout,
			DialKeepAliveTime: defaultDialKeepAlive,
			TLS: &tls.Config{
				RootCAs:              rootCAs,
				GetClientCertificate: getClientCertificate,
			},
		})
	} else {
		etcdClnt, err = clientv3.New(clientv3.Config{
			Endpoints:         etcdEndpoints,
			DialTimeout:       defaultDialTimeout,
			DialKeepAliveTime: defaultDialKeepAlive,
		})
	}
	return etcdClnt, err
}
