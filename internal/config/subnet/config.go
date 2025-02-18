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

package subnet

import (
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
)

const (
	baseURL    = "https://subnet.min.io"
	baseURLDev = "http://localhost:9000"
)

// DefaultKVS - default KV config for subnet settings
var DefaultKVS = config.KVS{
	config.KV{
		Key:   config.License, // Deprecated Dec 2021
		Value: "",
	},
	config.KV{
		Key:   config.APIKey,
		Value: "",
	},
	config.KV{
		Key:   config.Proxy,
		Value: "",
	},
}

// Config represents the subnet related configuration
type Config struct {
	// The subnet license token - Deprecated Dec 2021
	License string `json:"license"`

	// The subnet api key
	APIKey string `json:"apiKey"`

	// The HTTP(S) proxy URL to use for connecting to SUBNET
	Proxy string `json:"proxy"`

	// Transport configured with proxy_url if set optionally.
	transport http.RoundTripper

	// The subnet base URL
	BaseURL string
}

var configLock sync.RWMutex

// Registered indicates if cluster is registered or not
func (c *Config) Registered() bool {
	configLock.RLock()
	defer configLock.RUnlock()

	return len(c.APIKey) > 0
}

// ApplyEnv - applies the current subnet config to Console UI specific environment variables.
func (c *Config) ApplyEnv() {
	configLock.RLock()
	defer configLock.RUnlock()

	if c.License != "" {
		os.Setenv("CONSOLE_SUBNET_LICENSE", c.License)
	}
	if c.APIKey != "" {
		os.Setenv("CONSOLE_SUBNET_API_KEY", c.APIKey)
	}
	if c.Proxy != "" {
		os.Setenv("CONSOLE_SUBNET_PROXY", c.Proxy)
	}
	os.Setenv("CONSOLE_SUBNET_URL", c.BaseURL)
}

// Update - in-place update with new license and registration information.
func (c *Config) Update(ncfg Config, isDevEnv bool) {
	configLock.Lock()
	defer configLock.Unlock()

	c.License = ncfg.License
	c.APIKey = ncfg.APIKey
	c.Proxy = ncfg.Proxy
	c.transport = ncfg.transport
	c.BaseURL = baseURL

	if isDevEnv {
		c.BaseURL = os.Getenv("_MINIO_SUBNET_URL")
		if c.BaseURL == "" {
			c.BaseURL = baseURLDev
		}
	}
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS, transport http.RoundTripper) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.SubnetSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	var proxyURL *xnet.URL
	proxy := env.Get(config.EnvMinIOSubnetProxy, kvs.Get(config.Proxy))
	if len(proxy) > 0 {
		proxyURL, err = xnet.ParseHTTPURL(proxy)
		if err != nil {
			return cfg, err
		}
	}

	cfg.License = strings.TrimSpace(env.Get(config.EnvMinIOSubnetLicense, kvs.Get(config.License)))
	cfg.APIKey = strings.TrimSpace(env.Get(config.EnvMinIOSubnetAPIKey, kvs.Get(config.APIKey)))
	cfg.Proxy = proxy

	if transport == nil {
		// when transport is nil, it means we are just validating the
		// inputs not performing any network calls.
		return cfg, nil
	}

	// Make sure to clone the transport before editing the ProxyURL
	if proxyURL != nil {
		if tr, ok := transport.(*http.Transport); ok {
			ctransport := tr.Clone()
			ctransport.Proxy = http.ProxyURL((*url.URL)(proxyURL))
			cfg.transport = ctransport
		}
	} else {
		cfg.transport = transport
	}

	return cfg, nil
}
