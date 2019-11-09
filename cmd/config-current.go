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
	"strings"
	"sync"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/config/etcd"
	xldap "github.com/minio/minio/cmd/config/identity/ldap"
	"github.com/minio/minio/cmd/config/identity/openid"
	"github.com/minio/minio/cmd/config/notify"
	"github.com/minio/minio/cmd/config/policy/opa"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/target/http"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/env"
)

var (
	// globalServerConfig server config.
	globalServerConfig   config.Config
	globalServerConfigMu sync.RWMutex
)

func validateConfig(s config.Config) error {
	// Disable merging env values with config for validation.
	env.SetEnvOff()

	// Enable env values to validate KMS.
	defer env.SetEnvOn()

	if _, err := config.LookupCreds(s[config.CredentialsSubSys][config.Default]); err != nil {
		return err
	}

	if _, err := config.LookupRegion(s[config.RegionSubSys][config.Default]); err != nil {
		return err
	}

	if _, err := config.LookupWorm(s[config.WormSubSys][config.Default]); err != nil {
		return err
	}

	if globalIsXL {
		if _, err := storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default],
			globalXLSetDriveCount); err != nil {
			return err
		}
	}

	if _, err := cache.LookupConfig(s[config.CacheSubSys][config.Default]); err != nil {
		return err
	}

	if _, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default]); err != nil {
		return err
	}

	{
		etcdCfg, err := etcd.LookupConfig(s[config.EtcdSubSys][config.Default], globalRootCAs)
		if err != nil {
			return err
		}
		if etcdCfg.Enabled {
			etcdClnt, err := etcd.New(etcdCfg)
			if err != nil {
				return err
			}
			etcdClnt.Close()
		}
	}
	{
		kmsCfg, err := crypto.LookupConfig(s[config.KmsVaultSubSys][config.Default])
		if err != nil {
			return err
		}
		if kmsCfg.Vault.Enabled {
			// Set env to enable master key validation.
			// this is needed only for KMS.
			env.SetEnvOn()

			if _, err = crypto.NewKMS(kmsCfg); err != nil {
				return err
			}
		}
	}

	if _, err := openid.LookupConfig(s[config.IdentityOpenIDSubSys][config.Default],
		NewCustomHTTPTransport(), xhttp.DrainBody); err != nil {
		return err
	}

	if _, err := xldap.Lookup(s[config.IdentityLDAPSubSys][config.Default],
		globalRootCAs); err != nil {
		return err
	}

	if _, err := opa.LookupConfig(s[config.PolicyOPASubSys][config.Default],
		NewCustomHTTPTransport(), xhttp.DrainBody); err != nil {
		return err
	}

	if _, err := logger.LookupConfig(s); err != nil {
		return err
	}

	return notify.TestNotificationTargets(s, GlobalServiceDoneCh, globalRootCAs)
}

func lookupConfigs(s config.Config) (err error) {
	if !globalActiveCred.IsValid() {
		// Env doesn't seem to be set, we fallback to lookup creds from the config.
		globalActiveCred, err = config.LookupCreds(s[config.CredentialsSubSys][config.Default])
		if err != nil {
			return config.Errorf("Invalid credentials configuration: %s", err)
		}
	}

	etcdCfg, err := etcd.LookupConfig(s[config.EtcdSubSys][config.Default], globalRootCAs)
	if err != nil {
		return config.Errorf("Unable to initialize etcd config: %s", err)
	}

	globalEtcdClient, err = etcd.New(etcdCfg)
	if err != nil {
		return config.Errorf("Unable to initialize etcd config: %s", err)
	}

	if len(globalDomainNames) != 0 && !globalDomainIPs.IsEmpty() && globalEtcdClient != nil {
		globalDNSConfig, err = dns.NewCoreDNS(globalEtcdClient,
			dns.DomainNames(globalDomainNames),
			dns.DomainIPs(globalDomainIPs),
			dns.DomainPort(globalMinioPort),
			dns.CoreDNSPath(etcdCfg.CoreDNSPath),
		)
		if err != nil {
			return config.Errorf("Unable to initialize DNS config for %s: %s", globalDomainNames, err)
		}
	}

	globalServerRegion, err = config.LookupRegion(s[config.RegionSubSys][config.Default])
	if err != nil {
		return config.Errorf("Invalid region configuration: %s", err)
	}

	globalWORMEnabled, err = config.LookupWorm(s[config.WormSubSys][config.Default])
	if err != nil {
		return config.Errorf("Invalid worm configuration: %s", err)

	}

	if globalIsXL {
		globalStorageClass, err = storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default],
			globalXLSetDriveCount)
		if err != nil {
			return config.Errorf("Unable to initialize storage class config: %s", err)
		}
	}

	globalCacheConfig, err = cache.LookupConfig(s[config.CacheSubSys][config.Default])
	if err != nil {
		return config.Errorf("Unable to setup cache: %s", err)
	}

	if globalCacheConfig.Enabled {
		if cacheEncKey := env.Get(cache.EnvCacheEncryptionMasterKey, ""); cacheEncKey != "" {
			globalCacheKMS, err = crypto.ParseMasterKey(cacheEncKey)
			if err != nil {
				return config.Errorf("Unable to setup encryption cache: %s", err)
			}
		}
	}

	kmsCfg, err := crypto.LookupConfig(s[config.KmsVaultSubSys][config.Default])
	if err != nil {
		return config.Errorf("Unable to setup KMS config: %s", err)
	}

	GlobalKMS, err = crypto.NewKMS(kmsCfg)
	if err != nil {
		return config.Errorf("Unable to setup KMS with current KMS config: %s", err)
	}

	// Enable auto-encryption if enabled
	globalAutoEncryption = kmsCfg.AutoEncryption

	globalCompressConfig, err = compress.LookupConfig(s[config.CompressionSubSys][config.Default])
	if err != nil {
		return config.Errorf("Unable to setup Compression: %s", err)
	}

	globalOpenIDConfig, err = openid.LookupConfig(s[config.IdentityOpenIDSubSys][config.Default],
		NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		return config.Errorf("Unable to initialize OpenID: %s", err)
	}

	opaCfg, err := opa.LookupConfig(s[config.PolicyOPASubSys][config.Default],
		NewCustomHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		return config.Errorf("Unable to initialize OPA: %s", err)
	}

	globalOpenIDValidators = getOpenIDValidators(globalOpenIDConfig)
	globalPolicyOPA = opa.New(opaCfg)

	globalLDAPConfig, err = xldap.Lookup(s[config.IdentityLDAPSubSys][config.Default],
		globalRootCAs)
	if err != nil {
		return config.Errorf("Unable to parse LDAP configuration: %s", err)
	}

	// Load logger targets based on user's configuration
	loggerUserAgent := getUserAgent(getMinioMode())

	loggerCfg, err := logger.LookupConfig(s)
	if err != nil {
		return config.Errorf("Unable to initialize logger: %s", err)
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

	return nil
}

var helpMap = map[string]config.HelpKV{
	config.RegionSubSys:          config.RegionHelp,
	config.WormSubSys:            config.WormHelp,
	config.EtcdSubSys:            etcd.Help,
	config.CacheSubSys:           cache.Help,
	config.CompressionSubSys:     compress.Help,
	config.StorageClassSubSys:    storageclass.Help,
	config.IdentityOpenIDSubSys:  openid.Help,
	config.IdentityLDAPSubSys:    xldap.Help,
	config.PolicyOPASubSys:       opa.Help,
	config.KmsVaultSubSys:        crypto.Help,
	config.LoggerHTTPSubSys:      logger.Help,
	config.LoggerHTTPAuditSubSys: logger.HelpAudit,
	config.NotifyAMQPSubSys:      notify.HelpAMQP,
	config.NotifyKafkaSubSys:     notify.HelpKafka,
	config.NotifyMQTTSubSys:      notify.HelpMQTT,
	config.NotifyNATSSubSys:      notify.HelpNATS,
	config.NotifyNSQSubSys:       notify.HelpNSQ,
	config.NotifyMySQLSubSys:     notify.HelpMySQL,
	config.NotifyPostgresSubSys:  notify.HelpPostgres,
	config.NotifyRedisSubSys:     notify.HelpRedis,
	config.NotifyWebhookSubSys:   notify.HelpWebhook,
	config.NotifyESSubSys:        notify.HelpES,
}

// GetHelp - returns help for sub-sys, a key for a sub-system or all the help.
func GetHelp(subSys, key string, envOnly bool) (config.HelpKV, error) {
	if len(subSys) == 0 {
		return nil, config.Error("no help available for empty sub-system inputs")
	}
	subSystemValue := strings.SplitN(subSys, config.SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return nil, config.Errorf("invalid number of arguments %s", subSys)
	}

	if !config.SubSystems.Contains(subSystemValue[0]) {
		return nil, config.Errorf("unknown sub-system %s", subSys)
	}

	help := helpMap[subSystemValue[0]]
	if key != "" {
		value, ok := help[key]
		if !ok {
			return nil, config.Errorf("unknown key %s for sub-system %s", key, subSys)
		}
		help = config.HelpKV{
			key: value,
		}
	}

	envHelp := config.HelpKV{}
	if envOnly {
		for k, v := range help {
			envK := config.EnvPrefix + strings.Join([]string{
				strings.ToTitle(subSys), strings.ToTitle(k),
			}, config.EnvWordDelimiter)
			envHelp[envK] = v
		}
		help = envHelp
	}

	return help, nil
}

func configDefaultKVS() map[string]config.KVS {
	m := make(map[string]config.KVS)
	for k, tgt := range newServerConfig() {
		m[k] = tgt[config.Default]
	}
	return m
}

func newServerConfig() config.Config {
	srvCfg := config.New()
	for k := range srvCfg {
		// Initialize with default KVS
		switch k {
		case config.EtcdSubSys:
			srvCfg[k][config.Default] = etcd.DefaultKVS
		case config.CacheSubSys:
			srvCfg[k][config.Default] = cache.DefaultKVS
		case config.CompressionSubSys:
			srvCfg[k][config.Default] = compress.DefaultKVS
		case config.StorageClassSubSys:
			srvCfg[k][config.Default] = storageclass.DefaultKVS
		case config.IdentityLDAPSubSys:
			srvCfg[k][config.Default] = xldap.DefaultKVS
		case config.IdentityOpenIDSubSys:
			srvCfg[k][config.Default] = openid.DefaultKVS
		case config.PolicyOPASubSys:
			srvCfg[k][config.Default] = opa.DefaultKVS
		case config.WormSubSys:
			srvCfg[k][config.Default] = config.DefaultWormKVS
		case config.RegionSubSys:
			srvCfg[k][config.Default] = config.DefaultRegionKVS
		case config.CredentialsSubSys:
			srvCfg[k][config.Default] = config.DefaultCredentialKVS
		case config.KmsVaultSubSys:
			srvCfg[k][config.Default] = crypto.DefaultKVS
		case config.LoggerHTTPSubSys:
			srvCfg[k][config.Default] = logger.DefaultKVS
		case config.LoggerHTTPAuditSubSys:
			srvCfg[k][config.Default] = logger.DefaultAuditKVS
		}
	}
	for k, v := range notify.DefaultNotificationKVS {
		srvCfg[k][config.Default] = v
	}
	return srvCfg
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := newServerConfig()

	// Override any values from ENVs.
	if err := lookupConfigs(srvCfg); err != nil {
		return err
	}

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Save config into file.
	return saveServerConfig(context.Background(), objAPI, globalServerConfig, nil)
}

func getValidConfig(objAPI ObjectLayer) (config.Config, error) {
	srvCfg, err := readServerConfig(context.Background(), objAPI)
	if err != nil {
		return nil, err
	}
	defaultKVS := configDefaultKVS()
	for _, k := range config.SubSystems.ToSlice() {
		_, ok := srvCfg[k][config.Default]
		if !ok {
			// Populate default configs for any new
			// sub-systems added automatically.
			srvCfg[k][config.Default] = defaultKVS[k]
		}
	}
	return srvCfg, nil
}

// loadConfig - loads a new config from disk, overrides params
// from env if found and valid
func loadConfig(objAPI ObjectLayer) error {
	srvCfg, err := getValidConfig(objAPI)
	if err != nil {
		return err
	}

	// Override any values from ENVs.
	if err = lookupConfigs(srvCfg); err != nil {
		return err
	}

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
func getOpenIDValidators(cfg openid.Config) *openid.Validators {
	validators := openid.NewValidators()

	if cfg.JWKS.URL != nil {
		validators.Add(openid.NewJWT(cfg))
	}

	return validators
}
