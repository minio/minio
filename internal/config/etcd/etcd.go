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

package etcd

import (
	"crypto/tls"
	"crypto/x509"
	"strings"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
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
	// Disable etcd client SDK logging, etcd client
	// incorrectly starts logging in unexpected data
	// format.
	cfg.LogConfig = &zap.Config{
		Level:    zap.NewAtomicLevelAt(zap.FatalLevel),
		Encoding: "console",
	}
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
