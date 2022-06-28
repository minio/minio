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
	"strings"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
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
	APIKey string `json:"api_key"`

	// The HTTP(S) proxy URL to use for connecting to SUBNET
	ProxyURL *xnet.URL `json:"proxy_url"`

	// Transport configured with proxy_url if set optionally.
	transport *http.Transport
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS, transport http.RoundTripper) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.SubnetSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	proxy := env.Get(config.EnvMinIOSubnetProxy, kvs.Get(config.Proxy))
	if len(proxy) > 0 {
		cfg.ProxyURL, err = xnet.ParseHTTPURL(proxy)
		if err != nil {
			return cfg, err
		}

	}

	cfg.License = strings.TrimSpace(env.Get(config.EnvMinIOSubnetLicense, kvs.Get(config.License)))
	cfg.APIKey = strings.TrimSpace(env.Get(config.EnvMinIOSubnetAPIKey, kvs.Get(config.APIKey)))

	if transport == nil {
		// when transport is nil, it means we are just validating the
		// inputs not performing any network calls.
		return cfg, nil
	}

	// Make sure to clone the transport before editing the ProxyURL
	ctransport := transport.(*http.Transport).Clone()
	if cfg.ProxyURL != nil {
		ctransport.Proxy = http.ProxyURL((*url.URL)(cfg.ProxyURL))
	}
	cfg.transport = ctransport

	return cfg, nil
}
