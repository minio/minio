/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"sync"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	xldap "github.com/minio/minio/cmd/config/ldap"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/target/http"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/iam/openid"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

type serverConfig config.Config

var (
	// globalServerConfig server config.
	globalServerConfig   serverConfig
	globalServerConfigMu sync.RWMutex
)

// get current region.
func lookupRegion(kv config.KVS) string {
	return env.Get(config.EnvRegion, kv.Get(config.RegionName))
}

// check if worm is enabled
func lookupWorm(kv config.KVS) (bool, error) {
	worm := env.Get(config.EnvWorm, kv.Get(config.State))
	if worm == "" {
		return false, nil
	}
	return config.ParseBool(worm)
}

func (s serverConfig) lookupConfigs() {
	var err error
	globalWORMEnabled, err = lookupWorm(s[config.WormSubSys][config.Default])
	if err != nil {
		logger.Fatal(config.ErrInvalidWormValue(err),
			"Invalid worm configuration")
	}

	globalServerRegion = lookupRegion(s[config.RegionSubSys][config.Default])

	if globalIsXL {
		globalStorageClass, err = storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default],
			globalXLSetDriveCount)
		if err != nil {
			logger.FatalIf(err, "Unable to initialize storage class config")
		}
	}

	globalCacheConfig, err = cache.LookupConfig(s[config.CacheSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to setup cache")
	}

	if len(globalCacheConfig.Drives) > 0 {
		globalIsDiskCacheEnabled = true
		if cacheEncKey := env.Get(cache.EnvCacheEncryptionMasterKey, ""); cacheEncKey != "" {
			globalCacheKMS, err = crypto.ParseMasterKey(cacheEncKey)
			if err != nil {
				logger.FatalIf(config.ErrInvalidCacheEncryptionKey(err),
					"Unable to setup encryption cache")
			}
		}
	}

	kmsCfg, err := crypto.LookupConfig(s[config.KmsVaultSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to setup KMS config")
	}

	GlobalKMS, err = crypto.NewKMS(kmsCfg)
	if err != nil {
		logger.FatalIf(err, "Unable to setup KMS with current KMS config")
	}

	compCfg, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default])
	if err != nil {
		logger.FatalIf(err, "Unable to setup Compression")
	}

	if compCfg.Enabled {
		globalIsCompressionEnabled = compCfg.Enabled
		globalCompressExtensions = compCfg.Extensions
		globalCompressMimeTypes = compCfg.MimeTypes
	}

	openidCfg, err := openid.LookupConfig(s[config.IdentityOpenIDSubSys][config.Default],
		NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize OpenID")
	}

	opaCfg, err := iampolicy.LookupConfig(s[config.PolicyOPASubSys][config.Default],
		NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.FatalIf(err, "Unable to initialize OPA")
	}

	globalOpenIDValidators = getOpenIDValidators(openidCfg)
	globalPolicyOPA = iampolicy.NewOpa(opaCfg)

	globalLDAPConfig, err = xldap.Lookup(s[config.IdentityLDAPSubSys][config.Default],
		globalRootCAs)
	if err != nil {
		logger.FatalIf(err, "Unable to parse LDAP configuration from env")
	}

	// Load logger targets based on user's configuration
	loggerUserAgent := getUserAgent(getMinioMode())

	loggerCfg, err := logger.LookupConfig(config.Config(s))
	if err != nil {
		logger.FatalIf(err, "Unable to initialize logger")
	}

	for _, l := range loggerCfg.HTTP {
		if l.Enabled {
			// Enable http logging
			logger.AddTarget(http.New(l.Endpoint, loggerUserAgent, string(logger.All), NewCustomHTTPTransport()))
		}
	}

	for _, l := range loggerCfg.Audit {
		if l.Enabled {
			// Enable http audit logging
			logger.AddAuditTarget(http.New(l.Endpoint, loggerUserAgent, string(logger.All), NewCustomHTTPTransport()))
		}
	}

	// Enable console logging
	logger.AddTarget(globalConsoleSys.Console())
}

func newServerConfig() serverConfig {
	return serverConfig(config.New())
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := newServerConfig()

	// Override any values from ENVs.
	srvCfg.lookupConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Save config into file.
	return saveServerConfig(context.Background(), objAPI, globalServerConfig)
}

// getValidConfig - returns valid server configuration
func getValidConfig(objAPI ObjectLayer) (serverConfig, error) {
	srvCfg, err := readServerConfig(context.Background(), objAPI)
	if err != nil {
		return nil, err
	}

	return srvCfg, nil
}

// loadConfig - loads a new config from disk, overrides params from env
// if found and valid
func loadConfig(objAPI ObjectLayer) error {
	srvCfg, err := getValidConfig(objAPI)
	if err != nil {
		return config.ErrInvalidConfig(err)
	}

	// Override any values from ENVs.
	srvCfg.lookupConfigs()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	return nil
}

// getOpenIDValidators - returns ValidatorList which contains
// enabled providers in server config.
// A new authentication provider is added like below
// * Add a new provider in pkg/iam/openid package.
func getOpenIDValidators(args openid.JWKSArgs) *openid.Validators {
	validators := openid.NewValidators()

	if args.URL != nil {
		validators.Add(openid.NewJWT(args))
	}

	return validators
}
