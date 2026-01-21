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

	// EnvIdentityTLSSubjectSanURI is an environmental variable that is used to select
	// Subject for verified certificate identity in JWT Claim.
	// This claim is sent to Authorization Engine.
	// If set to true, List of SAN-URIs will be used as subject instead of CommonName
	// The URIs will be converted into suitable policy name by following operations
	// 1. remove protocol name (or scheme name) from URI
	// 2. Replace all Path separators (ie /) from the Path in URI, this results in CleanedPath
	// 3. Join Host+CleanedPath
	// 4. If the above string becomes greater than 128 characters in length, then
	// a proper error is thrown
	// 5. All characters are converted to lower case. and later checked for pattern '^[a-z0-9]+[a-z0-9+=.@_-]*$'
	// 6. The resulting string is used as policy name. There can be multiple SAN-URIs
	// in that case one of the matching policy will be
	// used to authorize the request.
	// As example, spiffe://my.domain:10000/my/app/path will be converted to
	// my.domain:10000_my_app_path

	// Valid values for this field are true and false
	// By default, it will be false. Thus Common Name will be used
	EnvIdentityTLSSubjectSanURI = "MINIO_IDENTITY_TLS_SUBJECT_USE_SANURI"
)

// Config contains the STS TLS configuration for generating temp.
// credentials and mapping client certificates to S3 policies.
type Config struct {
	Enabled bool `json:"enabled"`

	// InsecureSkipVerify, if set to true, disables the client
	// certificate verification. It should only be set for
	// debugging or testing purposes.
	InsecureSkipVerify bool `json:"skip_verify"`

	// TLSSubjectUseSANUri, if set to true, uses first SAN URI from
	// the client certificate as subject. This is done instead of
	// using Common Name.
	TLSSubjectUseSanURI bool `json:"use_san_uri"`
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

	cfg.TLSSubjectUseSanURI, err = config.ParseBool(env.Get(EnvIdentityTLSSubjectSanURI, kvs.Get(tlsSubjectUseSanURI)))
	if err != nil {
		return Config{}, err
	}

	return cfg, nil
}

const (
	skipVerify          = "skip_verify"
	tlsSubjectUseSanURI = "tls_subject_use_san_uri"
)

// DefaultKVS is the default K/V config system for
// the STS TLS API.
var DefaultKVS = config.KVS{
	config.KV{
		Key:   skipVerify,
		Value: "off",
	},
	config.KV{
		Key:   tlsSubjectUseSanURI,
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
	config.HelpKV{
		Key:         tlsSubjectUseSanURI,
		Description: `use cleaned values of one or more san uris from client certificate instead common name. cleaning results in stripping scheme, replacing path separator with underscore sign in uri path and joining it with host name. authorization will use one of the matching policies (default: 'off')`,
		Optional:    true,
		Type:        "on|off",
	},
}
