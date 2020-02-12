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
	"github.com/coreos/etcd/clientv3/namespace"
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
	PathPrefix    = "path_prefix"
	CoreDNSPath   = "coredns_path"
	ClientCert    = "client_cert"
	ClientCertKey = "client_cert_key"

	EnvEtcdEndpoints     = "MINIO_ETCD_ENDPOINTS"
	EnvEtcdPathPrefix    = "MINIO_ETCD_PATH_PREFIX"
	EnvEtcdCoreDNSPath   = "MINIO_ETCD_COREDNS_PATH"
	EnvEtcdClientCert    = "MINIO_ETCD_CLIENT_CERT"
	EnvEtcdClientCertKey = "MINIO_ETCD_CLIENT_CERT_KEY"
)

// DefaultKVS - default KV settings for etcd.
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   Endpoints,
			Value: "",
		},
		config.KV{
			Key:   PathPrefix,
			Value: "",
		},
		config.KV{
			Key:   CoreDNSPath,
			Value: "/skydns",
		},
		config.KV{
			Key:   ClientCert,
			Value: "",
		},
		config.KV{
			Key:   ClientCertKey,
			Value: "",
		},
	}
)

// Config - server etcd config.
type Config struct {
	Enabled     bool   `json:"enabled"`
	PathPrefix  string `json:"pathPrefix"`
	CoreDNSPath string `json:"coreDNSPath"`
	clientv3.Config
}

// New - initialize new etcd client.
func New(cfg Config) (*clientv3.Client, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cli, err := clientv3.New(cfg.Config)
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, cfg.PathPrefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, cfg.PathPrefix)
	cli.Lease = namespace.NewLease(cli.Lease, cfg.PathPrefix)
	return cli, nil
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
			return nil, false, config.Errorf("all endpoints should be https or http: %s", endpoint)
		}
		// If one of the endpoint is https, we will use https directly.
		etcdSecure = etcdSecure || u.Scheme == "https"
	}

	return etcdEndpoints, etcdSecure, nil
}

// Enabled returns if etcd is enabled.
func Enabled(kvs config.KVS) bool {
	endpoints := kvs.Get(Endpoints)
	return endpoints != ""
}

// LookupConfig - Initialize new etcd config.
func LookupConfig(kvs config.KVS, rootCAs *x509.CertPool) (Config, error) {
	cfg := Config{}
	if err := config.CheckValidKeys(config.EtcdSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	endpoints := env.Get(EnvEtcdEndpoints, kvs.Get(Endpoints))
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
	cfg.CoreDNSPath = env.Get(EnvEtcdCoreDNSPath, kvs.Get(CoreDNSPath))
	// Default path prefix for all keys on etcd, other than CoreDNSPath.
	cfg.PathPrefix = env.Get(EnvEtcdPathPrefix, kvs.Get(PathPrefix))
	if etcdSecure {
		cfg.TLS = &tls.Config{
			RootCAs: rootCAs,
		}
		// This is only to support client side certificate authentication
		// https://coreos.com/etcd/docs/latest/op-guide/security.html
		etcdClientCertFile := env.Get(EnvEtcdClientCert, kvs.Get(ClientCert))
		etcdClientCertKey := env.Get(EnvEtcdClientCertKey, kvs.Get(ClientCertKey))
		if etcdClientCertFile != "" && etcdClientCertKey != "" {
			cfg.TLS.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := tls.LoadX509KeyPair(etcdClientCertFile, etcdClientCertKey)
				return &cert, err
			}
		}
	}
	return cfg, nil
}
