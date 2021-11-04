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
	jwtgo "github.com/golang-jwt/jwt/v4"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

var (
	// DefaultKVS - default KV config for subnet settings
	DefaultKVS = config.KVS{
		config.KV{
			Key:   config.License,
			Value: "",
		},
	}

	// HelpLicense - provides help for license config
	HelpLicense = config.HelpKVS{
		config.HelpKV{
			Key:         config.License,
			Type:        "string",
			Description: "Subnet license token for the cluster",
			Optional:    true,
		},
	}
)

// Config represents the subnet related configuration
type Config struct {
	// The subnet license token
	License string `json:"license"`
}

func validateLicenseFormat(lic string) error {
	if len(lic) == 0 {
		return nil
	}

	// Only verifying that the string is a parseable JWT token as of now
	_, _, err := new(jwtgo.Parser).ParseUnverified(lic, jwtgo.MapClaims{})
	return err
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.SubnetSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.License = env.Get(config.EnvMinIOSubnetLicense, kvs.Get(config.License))

	return cfg, validateLicenseFormat(cfg.License)
}
