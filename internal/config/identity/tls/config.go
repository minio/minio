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

package tls

import (
	"errors"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

const (
	// EnvEnabled is an environment variable that controls whether the X.509
	// TLS STS API is enabled. By default, if not set, it is enabled.
	EnvEnabled = "MINIO_IDENTITY_TLS_ENABLE"

	// EnvSTSExpiry is an environment variable for the expiry of temp.
	// generated STS credentials. Its minimum value is 15 minutes and
	// defaults to 1 hour, if not set.
	EnvSTSExpiry = "MINIO_IDENTITY_TLS_STS_EXPIRY"

	// EnvSkipVerify is an environment variable that controls whether
	// MinIO verifies the client certificate present by the client
	// when requesting temp. credentials.
	// By default, MinIO always verify the client certificate.
	//
	// The client certificate verification should only be skipped
	// when debugging or testing a setup since it allows arbitrary
	// clients to obtain temp. credentials with arbitrary policy
	// permissions - including admin permissions.
	EnvSkipVerify = "MINIO_IDENTITY_TLS_SKIP_VERIFY"
)

// Config contains the STS TLS configuration for generating temp.
// credentials and mapping client certificates to S3 policies.
type Config struct {
	Enabled bool `json:"enabled"`

	// STSExpiry is the expiry duration of generated credentials.
	STSExpiry time.Duration `json:"sts_expiry"`

	// InsecureSkipVerify, if set to true, disables the client
	// certificate verification. It should only be set for
	// debugging or testing purposes.
	InsecureSkipVerify bool `json:"skip_verify"`
}

// Lookup returns a new Config by merging the given K/V config
// system with environment variables.
func Lookup(kvs config.KVS) (Config, error) {
	if err := config.CheckValidKeys(config.IdentityTLSSubSys, kvs, DefaultKVS); err != nil {
		return Config{}, err
	}
	expiry, err := time.ParseDuration(env.Get(EnvSTSExpiry, kvs.Get(stsExpiry)))
	if err != nil {
		return Config{}, err
	}
	if expiry < 0 {
		return Config{}, errors.New("tls: STS expiry must not be negative")
	}
	if expiry == 0 {
		expiry = 1 * time.Hour // Set default expiry, if empty
	}
	if expiry < 15*time.Minute {
		expiry = 15 * time.Minute // The minimal expiry duration is 15 minutes
	}
	insecureSkipVerify, err := config.ParseBool(env.Get(EnvSkipVerify, kvs.Get(skipVerify)))
	if err != nil {
		return Config{}, err
	}
	enabled, err := config.ParseBool(env.Get(EnvEnabled, "on"))
	if err != nil {
		return Config{}, err
	}
	return Config{
		Enabled:            enabled,
		STSExpiry:          expiry,
		InsecureSkipVerify: insecureSkipVerify,
	}, nil
}

const (
	stsExpiry  = "sts_expiry"
	skipVerify = "skip_verify"
)

// DefaultKVS is the the default K/V config system for
// the STS TLS API.
var DefaultKVS = config.KVS{
	config.KV{
		Key:   stsExpiry,
		Value: (1 * time.Hour).String(),
	},
	config.KV{
		Key:   skipVerify,
		Value: "off",
	},
}

// Help is the help and description for the STS API K/V configuration.
var Help = config.HelpKVS{
	config.HelpKV{
		Key:         stsExpiry,
		Description: `temporary credentials validity duration in s,m,h,d. Default is "1h"`,
		Optional:    true,
		Type:        "duration",
	},
	config.HelpKV{
		Key:         skipVerify,
		Description: `trust client certificates without verification. Defaults to "off" (verify)`,
		Optional:    true,
		Type:        "on|off",
	},
}
