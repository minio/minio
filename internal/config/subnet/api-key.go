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
	"github.com/google/uuid"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

var (
	// DefaultKVS - default KV config for subnet settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.APIKey,
			Value: "",
		},
	}

	// HelpAPIKey - provides help for subnet api key config
	HelpAPIKey = config.HelpKVS{
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
	// The subnet api key
	APIKey string `json:"api_key"`
}

func validateAPIKeyFormat(apiKey string) error {
	if len(apiKey) == 0 {
		return nil
	}

	// Verify that the api key is a valid UUID string
	_, err := uuid.Parse(apiKey)
	return err
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.SubnetSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.APIKey = env.Get(config.EnvMinIOSubnetAPIKey, kvs.Get(config.APIKey))

	return cfg, validateAPIKeyFormat(cfg.APIKey)
}
