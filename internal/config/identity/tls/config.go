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
	"strconv"
	"time"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

const (
	// EnvIdentityTLSEnabled is an environment variable that controls whether the X.509
	// TLS STS API is enabled. By default, if not set, it is enabled.
	EnvIdentityTLSEnabled = "MINIO_IDENTITY_TLS_ENABLE"

	// EnvIdentityTLSSkipVerify is an environment variable that controls whether
	// MinIO verifies the client certificate present by the client
	// when requesting temp. credentials.
	// By default, MinIO always verify the client certificate.
	//
	// The client certificate verification should only be skipped
	// when debugging or testing a setup since it allows arbitrary
	// clients to obtain temp. credentials with arbitrary policy
	// permissions - including admin permissions.
	EnvIdentityTLSSkipVerify = "MINIO_IDENTITY_TLS_SKIP_VERIFY"
)

// Config contains the STS TLS configuration for generating temp.
// credentials and mapping client certificates to S3 policies.
type Config struct {
	Enabled bool `json:"enabled"`

	// InsecureSkipVerify, if set to true, disables the client
	// certificate verification. It should only be set for
	// debugging or testing purposes.
	InsecureSkipVerify bool `json:"skip_verify"`
}

const (
	defaultExpiry time.Duration = 1 * time.Hour
	minExpiry     time.Duration = 15 * time.Minute
	maxExpiry     time.Duration = 365 * 24 * time.Hour
)

// GetExpiryDuration - return parsed expiry duration.
func (l Config) GetExpiryDuration(dsecs string) (time.Duration, error) {
	if dsecs == "" {
		return defaultExpiry, nil
	}

	d, err := strconv.Atoi(dsecs)
	if err != nil {
		return 0, auth.ErrInvalidDuration
	}

	dur := time.Duration(d) * time.Second

	if dur < minExpiry || dur > maxExpiry {
		return 0, auth.ErrInvalidDuration
	}
	return dur, nil
}

// Lookup returns a new Config by merging the given K/V config
// system with environment variables.
func Lookup(kvs config.KVS) (Config, error) {
	if err := config.CheckValidKeys(config.IdentityTLSSubSys, kvs, DefaultKVS); err != nil {
		return Config{}, err
	}
	cfg := Config{}
	var err error
	v := env.Get(EnvIdentityTLSEnabled, "")
	if v == "" {
		return cfg, nil
	}
	cfg.Enabled, err = config.ParseBool(v)
	if err != nil {
		return Config{}, err
	}
	cfg.InsecureSkipVerify, err = config.ParseBool(env.Get(EnvIdentityTLSSkipVerify, kvs.Get(skipVerify)))
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}

const (
	skipVerify = "skip_verify"
)

// DefaultKVS is the default K/V config system for
// the STS TLS API.
var DefaultKVS = config.KVS{
	config.KV{
		Key:   skipVerify,
		Value: "off",
	},
}

// Help is the help and description for the STS API K/V configuration.
var Help = config.HelpKVS{
	config.HelpKV{
		Key:         skipVerify,
		Description: `trust client certificates without verification (default: 'off')`,
		Optional:    true,
		Type:        "on|off",
	},
}
