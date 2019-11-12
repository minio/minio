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
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	// Default values used while communicating with etcd.
	defaultDialTimeout   = 5 * time.Second
	defaultDialKeepAlive = 30 * time.Second
)

// etcd environment values
const (
	Endpoints     = "endpoints"
	CoreDNSPath   = "coredns_path"
	ClientCert    = "client_cert"
	ClientCertKey = "client_cert_key"

	EnvEtcdState         = "MINIO_ETCD_STATE"
	EnvEtcdEndpoints     = "MINIO_ETCD_ENDPOINTS"
	EnvEtcdCoreDNSPath   = "MINIO_ETCD_COREDNS_PATH"
	EnvEtcdClientCert    = "MINIO_ETCD_CLIENT_CERT"
	EnvEtcdClientCertKey = "MINIO_ETCD_CLIENT_CERT_KEY"
)

// DefaultKVS - default KV settings for etcd.
var (
	DefaultKVS = config.KVS{
		config.State:   config.StateOff,
		config.Comment: "This is a default etcd configuration",
		Endpoints:      "",
		CoreDNSPath:    "/skydns",
		ClientCert:     "",
		ClientCertKey:  "",
	}
)

// Config - server etcd config.
type Config struct {
	Enabled     bool   `json:"enabled"`
	CoreDNSPath string `json:"coreDNSPath"`
	clientv3.Config
}

// New - initialize new etcd client.
func New(cfg Config) (*clientv3.Client, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	return clientv3.New(cfg.Config)
}

func parseEndpoints(endpoints string) ([]string, bool, error) {
	etcdEndpoints := strings.Split(endpoints, config.ValueSeparator)

	var etcdSecure bool
	for _, endpoint := range etcdEndpoints {
		u, err := xnet.ParseHTTPURL(endpoint)
		if err != nil {
			return nil, false, err
		}
		if etcdSecure && u.Scheme == "http" {
			return nil, false, fmt.Errorf("all endpoints should be https or http: %s", endpoint)
		}
		// If one of the endpoint is https, we will use https directly.
		etcdSecure = etcdSecure || u.Scheme == "https"
	}

	return etcdEndpoints, etcdSecure, nil
}

func lookupLegacyConfig(rootCAs *x509.CertPool) (Config, error) {
	cfg := Config{}
	endpoints := env.Get(EnvEtcdEndpoints, "")
	if endpoints == "" {
		return cfg, nil
	}
	etcdEndpoints, etcdSecure, err := parseEndpoints(endpoints)
	if err != nil {
		return cfg, err
	}
	cfg.Enabled = true
	cfg.DialTimeout = defaultDialTimeout
	cfg.DialKeepAliveTime = defaultDialKeepAlive
	cfg.Endpoints = etcdEndpoints
	cfg.CoreDNSPath = "/skydns"
	if etcdSecure {
		cfg.TLS = &tls.Config{
			RootCAs: rootCAs,
		}
		// This is only to support client side certificate authentication
		// https://coreos.com/etcd/docs/latest/op-guide/security.html
		etcdClientCertFile := env.Get(EnvEtcdClientCert, "")
		etcdClientCertKey := env.Get(EnvEtcdClientCertKey, "")
		if etcdClientCertFile != "" && etcdClientCertKey != "" {
			cfg.TLS.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := tls.LoadX509KeyPair(etcdClientCertFile, etcdClientCertKey)
				return &cert, err
			}
		}
	}
	return cfg, nil
}

// LookupConfig - Initialize new etcd config.
func LookupConfig(kv config.KVS, rootCAs *x509.CertPool) (Config, error) {
	cfg := Config{}
	if err := config.CheckValidKeys(config.EtcdSubSys, kv, DefaultKVS); err != nil {
		return cfg, err
	}

	stateBool, err := config.ParseBool(env.Get(EnvEtcdState, config.StateOn))
	if err != nil {
		return cfg, err
	}

	if stateBool {
		// By default state is 'on' to honor legacy config.
		cfg, err = lookupLegacyConfig(rootCAs)
		if err != nil {
			return cfg, err
		}
		// If old legacy config is enabled honor it.
		if cfg.Enabled {
			return cfg, nil
		}
	}

	stateBool, err = config.ParseBool(env.Get(EnvEtcdState, kv.Get(config.State)))
	if err != nil {
		return cfg, err
	}

	if !stateBool {
		return cfg, nil
	}

	endpoints := env.Get(EnvEtcdEndpoints, kv.Get(Endpoints))
	if endpoints == "" {
		return cfg, config.Error("'endpoints' key cannot be empty to enable etcd")
	}

	etcdEndpoints, etcdSecure, err := parseEndpoints(endpoints)
	if err != nil {
		return cfg, err
	}

	cfg.Enabled = true
	cfg.DialTimeout = defaultDialTimeout
	cfg.DialKeepAliveTime = defaultDialKeepAlive
	cfg.Endpoints = etcdEndpoints
	cfg.CoreDNSPath = env.Get(EnvEtcdCoreDNSPath, kv.Get(CoreDNSPath))
	if etcdSecure {
		cfg.TLS = &tls.Config{
			RootCAs: rootCAs,
		}
		// This is only to support client side certificate authentication
		// https://coreos.com/etcd/docs/latest/op-guide/security.html
		etcdClientCertFile := env.Get(EnvEtcdClientCert, kv.Get(ClientCert))
		etcdClientCertKey := env.Get(EnvEtcdClientCertKey, kv.Get(ClientCertKey))
		if etcdClientCertFile != "" && etcdClientCertKey != "" {
			cfg.TLS.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := tls.LoadX509KeyPair(etcdClientCertFile, etcdClientCertKey)
				return &cert, err
			}
		}
	}
	return cfg, nil
}
