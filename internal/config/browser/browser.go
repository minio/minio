// Copyright (c) 2015-2023 MinIO, Inc.
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

package browser

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// Browser sub-system constants
const (
	// browserCSPPolicy setting name for Content-Security-Policy response header value
	browserCSPPolicy = "csp_policy"
	// browserHSTSSeconds setting name for Strict-Transport-Security response header, amount of seconds for 'max-age'
	browserHSTSSeconds = "hsts_seconds"
	// browserHSTSIncludeSubdomains setting name for Strict-Transport-Security response header 'includeSubDomains' flag (true or false)
	browserHSTSIncludeSubdomains = "hsts_include_subdomains"
	// browserHSTSPreload setting name for Strict-Transport-Security response header 'preload' flag (true or false)
	browserHSTSPreload = "hsts_preload"
	// browserReferrerPolicy setting name for Referrer-Policy response header
	browserReferrerPolicy = "referrer_policy"

	EnvBrowserCSPPolicy             = "MINIO_BROWSER_CONTENT_SECURITY_POLICY"
	EnvBrowserHSTSSeconds           = "MINIO_BROWSER_HSTS_SECONDS"
	EnvBrowserHSTSIncludeSubdomains = "MINIO_BROWSER_HSTS_INCLUDE_SUB_DOMAINS"
	EnvBrowserHSTSPreload           = "MINIO_BROWSER_HSTS_PRELOAD"
	EnvBrowserReferrerPolicy        = "MINIO_BROWSER_REFERRER_POLICY"
)

// DefaultKVS - default storage class config
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   browserCSPPolicy,
			Value: "default-src 'self' 'unsafe-eval' 'unsafe-inline'; script-src 'self' https://unpkg.com;  connect-src 'self' https://unpkg.com;",
		},
		config.KV{
			Key:   browserHSTSSeconds,
			Value: "0",
		},
		config.KV{
			Key:   browserHSTSIncludeSubdomains,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   browserHSTSPreload,
			Value: config.EnableOff,
		},
		config.KV{
			Key:   browserReferrerPolicy,
			Value: "strict-origin-when-cross-origin",
		},
	}
)

// configLock is a global lock for browser config
var configLock sync.RWMutex

// Config storage class configuration
type Config struct {
	CSPPolicy             string `json:"csp_policy"`
	HSTSSeconds           int    `json:"hsts_seconds"`
	HSTSIncludeSubdomains bool   `json:"hsts_include_subdomains"`
	HSTSPreload           bool   `json:"hsts_preload"`
	ReferrerPolicy        string `json:"referrer_policy"`
}

// Update Updates browser with new config
func (browseCfg *Config) Update(newCfg Config) {
	configLock.Lock()
	defer configLock.Unlock()
	browseCfg.CSPPolicy = newCfg.CSPPolicy
	browseCfg.HSTSSeconds = newCfg.HSTSSeconds
	browseCfg.HSTSIncludeSubdomains = newCfg.HSTSIncludeSubdomains
	browseCfg.HSTSPreload = newCfg.HSTSPreload
	browseCfg.ReferrerPolicy = newCfg.ReferrerPolicy
}

// LookupConfig - lookup api config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	cfg = Config{
		CSPPolicy:             env.Get(EnvBrowserCSPPolicy, kvs.GetWithDefault(browserCSPPolicy, DefaultKVS)),
		HSTSSeconds:           0,
		HSTSIncludeSubdomains: true,
		HSTSPreload:           true,
		ReferrerPolicy:        "strict-origin-when-cross-origin",
	}

	if err = config.CheckValidKeys(config.BrowserSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	hstsIncludeSubdomains := env.Get(EnvBrowserHSTSIncludeSubdomains, kvs.GetWithDefault(browserHSTSIncludeSubdomains, DefaultKVS)) == config.EnableOn
	hstsPreload := env.Get(EnvBrowserHSTSPreload, kvs.Get(browserHSTSPreload)) == config.EnableOn

	hstsSeconds, err := strconv.Atoi(env.Get(EnvBrowserHSTSSeconds, kvs.GetWithDefault(browserHSTSSeconds, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	cfg.HSTSSeconds = hstsSeconds
	cfg.HSTSIncludeSubdomains = hstsIncludeSubdomains
	cfg.HSTSPreload = hstsPreload

	referrerPolicy := env.Get(EnvBrowserReferrerPolicy, kvs.GetWithDefault(browserReferrerPolicy, DefaultKVS))
	switch referrerPolicy {
	case "no-referrer", "no-referrer-when-downgrade", "origin", "origin-when-cross-origin", "same-origin", "strict-origin", "strict-origin-when-cross-origin", "unsafe-url":
		cfg.ReferrerPolicy = referrerPolicy
	default:
		return cfg, fmt.Errorf("invalid value %v for %s", referrerPolicy, browserReferrerPolicy)
	}

	return cfg, nil
}

// GetCSPolicy - Get the Content security Policy
func (browseCfg *Config) GetCSPolicy() string {
	configLock.RLock()
	defer configLock.RUnlock()
	return browseCfg.CSPPolicy
}

// GetHSTSSeconds - Get the Content security Policy
func (browseCfg *Config) GetHSTSSeconds() int {
	configLock.RLock()
	defer configLock.RUnlock()
	return browseCfg.HSTSSeconds
}

// IsHSTSIncludeSubdomains - is HSTS 'includeSubdomains' directive enabled
func (browseCfg *Config) IsHSTSIncludeSubdomains() string {
	configLock.RLock()
	defer configLock.RUnlock()
	if browseCfg.HSTSSeconds > 0 && browseCfg.HSTSIncludeSubdomains {
		return config.EnableOn
	}
	return config.EnableOff
}

// IsHSTSPreload - is HSTS 'preload' directive enabled
func (browseCfg *Config) IsHSTSPreload() string {
	configLock.RLock()
	defer configLock.RUnlock()
	if browseCfg.HSTSSeconds > 0 && browseCfg.HSTSPreload {
		return config.EnableOn
	}
	return config.EnableOff
}

// GetReferPolicy - Get the ReferPolicy
func (browseCfg *Config) GetReferPolicy() string {
	configLock.RLock()
	defer configLock.RUnlock()
	return browseCfg.ReferrerPolicy
}
