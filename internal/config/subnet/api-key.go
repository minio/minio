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
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

var (
	// DefaultKVS - default KV config for subnet settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.License, // Deprecated Dec 2021
			Value: "",
		},
		config.KV{
			Key:   config.APIKey,
			Value: "",
		},
	}

	// HelpSubnet - provides help for subnet api key config
	HelpSubnet = config.HelpKVS{
		config.HelpKV{
			Key:         config.License, // Deprecated Dec 2021
			Type:        "string",
			Description: "[DEPRECATED use api_key] Subnet license token for the cluster",
			Optional:    true,
		},
		config.HelpKV{
			Key:         config.APIKey,
			Type:        "string",
			Description: "Subnet api key for the cluster",
			Optional:    true,
		},
	}
)

// Config represents the subnet related configuration
type Config struct {
	// The subnet license token - Deprecated Dec 2021
	License string `json:"license"`

	// The subnet api key
	APIKey string `json:"api_key"`
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.SubnetSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.License = env.Get(config.EnvMinIOSubnetLicense, kvs.Get(config.License))
	cfg.APIKey = env.Get(config.EnvMinIOSubnetAPIKey, kvs.Get(config.APIKey))

	return cfg, nil
}
