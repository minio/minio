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
	"net/url"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
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
	Proxy string `json:"proxy"`
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.SubnetSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.Proxy = env.Get(config.EnvMinIOSubnetProxy, kvs.Get(config.Proxy))
	_, err = url.Parse(cfg.Proxy)
	if err != nil {
		return cfg, err
	}

	cfg.License = env.Get(config.EnvMinIOSubnetLicense, kvs.Get(config.License))
	cfg.APIKey = env.Get(config.EnvMinIOSubnetAPIKey, kvs.Get(config.APIKey))

	return cfg, nil
}
