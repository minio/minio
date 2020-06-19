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
	"fmt"
	"strings"
	"sync"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/api"
	"github.com/minio/minio/cmd/config/cache"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/config/etcd"
	"github.com/minio/minio/cmd/config/etcd/dns"
	xldap "github.com/minio/minio/cmd/config/identity/ldap"
	"github.com/minio/minio/cmd/config/identity/openid"
	"github.com/minio/minio/cmd/config/notify"
	"github.com/minio/minio/cmd/config/policy/opa"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/target/http"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/madmin"
)

func initHelp() {
	var kvs = map[string]config.KVS{
		config.EtcdSubSys:           etcd.DefaultKVS,
		config.CacheSubSys:          cache.DefaultKVS,
		config.CompressionSubSys:    compress.DefaultKVS,
		config.IdentityLDAPSubSys:   xldap.DefaultKVS,
		config.IdentityOpenIDSubSys: openid.DefaultKVS,
		config.PolicyOPASubSys:      opa.DefaultKVS,
		config.RegionSubSys:         config.DefaultRegionKVS,
		config.APISubSys:            api.DefaultKVS,
		config.CredentialsSubSys:    config.DefaultCredentialKVS,
		config.KmsVaultSubSys:       crypto.DefaultVaultKVS,
		config.KmsKesSubSys:         crypto.DefaultKesKVS,
		config.LoggerWebhookSubSys:  logger.DefaultKVS,
		config.AuditWebhookSubSys:   logger.DefaultAuditKVS,
	}
	for k, v := range notify.DefaultNotificationKVS {
		kvs[k] = v
	}
	if globalIsErasure {
		kvs[config.StorageClassSubSys] = storageclass.DefaultKVS
	}
	config.RegisterDefaultKVS(kvs)

	// Captures help for each sub-system
	var helpSubSys = config.HelpKVS{
		config.HelpKV{
			Key:         config.RegionSubSys,
			Description: "label the location of the server",
		},
		config.HelpKV{
			Key:         config.CacheSubSys,
			Description: "add caching storage tier",
		},
		config.HelpKV{
			Key:         config.CompressionSubSys,
			Description: "enable server side compression of objects",
		},
		config.HelpKV{
			Key:         config.EtcdSubSys,
			Description: "federate multiple clusters for IAM and Bucket DNS",
		},
		config.HelpKV{
			Key:         config.IdentityOpenIDSubSys,
			Description: "enable OpenID SSO support",
		},
		config.HelpKV{
			Key:         config.IdentityLDAPSubSys,
			Description: "enable LDAP SSO support",
		},
		config.HelpKV{
			Key:         config.PolicyOPASubSys,
			Description: "enable external OPA for policy enforcement",
		},
		config.HelpKV{
			Key:         config.KmsVaultSubSys,
			Description: "enable external HashiCorp Vault key management service",
		},
		config.HelpKV{
			Key:         config.KmsKesSubSys,
			Description: "enable external MinIO key encryption service",
		},
		config.HelpKV{
			Key:         config.APISubSys,
			Description: "manage global HTTP API call specific features, such as throttling, authentication types, etc.",
		},
		config.HelpKV{
			Key:             config.LoggerWebhookSubSys,
			Description:     "send server logs to webhook endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.AuditWebhookSubSys,
			Description:     "send audit logs to webhook endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyWebhookSubSys,
			Description:     "publish bucket notifications to webhook endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyAMQPSubSys,
			Description:     "publish bucket notifications to AMQP endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyKafkaSubSys,
			Description:     "publish bucket notifications to Kafka endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyMQTTSubSys,
			Description:     "publish bucket notifications to MQTT endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyNATSSubSys,
			Description:     "publish bucket notifications to NATS endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyNSQSubSys,
			Description:     "publish bucket notifications to NSQ endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyMySQLSubSys,
			Description:     "publish bucket notifications to MySQL databases",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyPostgresSubSys,
			Description:     "publish bucket notifications to Postgres databases",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyESSubSys,
			Description:     "publish bucket notifications to Elasticsearch endpoints",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:             config.NotifyRedisSubSys,
			Description:     "publish bucket notifications to Redis datastores",
			MultipleTargets: true,
		},
	}

	if globalIsErasure {
		helpSubSys = append(helpSubSys, config.HelpKV{})
		copy(helpSubSys[2:], helpSubSys[1:])
		helpSubSys[1] = config.HelpKV{
			Key:         config.StorageClassSubSys,
			Description: "define object level redundancy",
		}
	}

	var helpMap = map[string]config.HelpKVS{
		"":                          helpSubSys, // Help for all sub-systems.
		config.RegionSubSys:         config.RegionHelp,
		config.APISubSys:            api.Help,
		config.StorageClassSubSys:   storageclass.Help,
		config.EtcdSubSys:           etcd.Help,
		config.CacheSubSys:          cache.Help,
		config.CompressionSubSys:    compress.Help,
		config.IdentityOpenIDSubSys: openid.Help,
		config.IdentityLDAPSubSys:   xldap.Help,
		config.PolicyOPASubSys:      opa.Help,
		config.KmsVaultSubSys:       crypto.HelpVault,
		config.KmsKesSubSys:         crypto.HelpKes,
		config.LoggerWebhookSubSys:  logger.Help,
		config.AuditWebhookSubSys:   logger.HelpAudit,
		config.NotifyAMQPSubSys:     notify.HelpAMQP,
		config.NotifyKafkaSubSys:    notify.HelpKafka,
		config.NotifyMQTTSubSys:     notify.HelpMQTT,
		config.NotifyNATSSubSys:     notify.HelpNATS,
		config.NotifyNSQSubSys:      notify.HelpNSQ,
		config.NotifyMySQLSubSys:    notify.HelpMySQL,
		config.NotifyPostgresSubSys: notify.HelpPostgres,
		config.NotifyRedisSubSys:    notify.HelpRedis,
		config.NotifyWebhookSubSys:  notify.HelpWebhook,
		config.NotifyESSubSys:       notify.HelpES,
	}

	config.RegisterHelpSubSys(helpMap)
}

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

	if _, err := api.LookupConfig(s[config.APISubSys][config.Default]); err != nil {
		return err
	}

	if globalIsErasure {
		if _, err := storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default],
			globalErasureSetDriveCount); err != nil {
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
		kmsCfg, err := crypto.LookupConfig(s, globalCertsCADir.Get(), NewGatewayHTTPTransport())
		if err != nil {
			return err
		}

		// Set env to enable master key validation.
		// this is needed only for KMS.
		env.SetEnvOn()

		if _, err = crypto.NewKMS(kmsCfg); err != nil {
			return err
		}

		// Disable merging env values for the rest.
		env.SetEnvOff()
	}

	if _, err := openid.LookupConfig(s[config.IdentityOpenIDSubSys][config.Default],
		NewGatewayHTTPTransport(), xhttp.DrainBody); err != nil {
		return err
	}

	{
		cfg, err := xldap.Lookup(s[config.IdentityLDAPSubSys][config.Default],
			globalRootCAs)
		if err != nil {
			return err
		}
		if cfg.Enabled {
			conn, cerr := cfg.Connect()
			if cerr != nil {
				return cerr
			}
			conn.Close()
		}
	}

	if _, err := opa.LookupConfig(s[config.PolicyOPASubSys][config.Default],
		NewGatewayHTTPTransport(), xhttp.DrainBody); err != nil {
		return err
	}

	if _, err := logger.LookupConfig(s); err != nil {
		return err
	}

	return notify.TestNotificationTargets(s, GlobalContext.Done(), NewGatewayHTTPTransport(),
		globalNotificationSys.ConfiguredTargetIDs())
}

func lookupConfigs(s config.Config) {
	ctx := GlobalContext

	var err error
	if !globalActiveCred.IsValid() {
		// Env doesn't seem to be set, we fallback to lookup creds from the config.
		globalActiveCred, err = config.LookupCreds(s[config.CredentialsSubSys][config.Default])
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Invalid credentials configuration: %w", err))
		}
	}

	etcdCfg, err := etcd.LookupConfig(s[config.EtcdSubSys][config.Default], globalRootCAs)
	if err != nil {
		if globalIsGateway {
			logger.FatalIf(err, "Unable to initialize etcd config")
		} else {
			logger.LogIf(ctx, fmt.Errorf("Unable to initialize etcd config: %w", err))
		}
	}

	if etcdCfg.Enabled {
		if globalEtcdClient == nil {
			globalEtcdClient, err = etcd.New(etcdCfg)
			if err != nil {
				if globalIsGateway {
					logger.FatalIf(err, "Unable to initialize etcd config")
				} else {
					logger.LogIf(ctx, fmt.Errorf("Unable to initialize etcd config: %w", err))
				}
			}
		}

		if len(globalDomainNames) != 0 && !globalDomainIPs.IsEmpty() && globalEtcdClient != nil && globalDNSConfig == nil {
			globalDNSConfig, err = dns.NewCoreDNS(etcdCfg.Config,
				dns.DomainNames(globalDomainNames),
				dns.DomainIPs(globalDomainIPs),
				dns.DomainPort(globalMinioPort),
				dns.CoreDNSPath(etcdCfg.CoreDNSPath),
			)
			if err != nil {
				if globalIsGateway {
					logger.FatalIf(err, "Unable to initialize DNS config")
				} else {
					logger.LogIf(ctx, fmt.Errorf("Unable to initialize DNS config for %s: %w",
						globalDomainNames, err))
				}
			}
		}
	}

	// Bucket federation is 'true' only when IAM assets are not namespaced
	// per tenant and all tenants interested in globally available users
	// if namespace was requested such as specifying etcdPathPrefix then
	// we assume that users are interested in global bucket support
	// but not federation.
	globalBucketFederation = etcdCfg.PathPrefix == "" && etcdCfg.Enabled

	globalServerRegion, err = config.LookupRegion(s[config.RegionSubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Invalid region configuration: %w", err))
	}

	apiConfig, err := api.LookupConfig(s[config.APISubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Invalid api configuration: %w", err))
	}

	globalAPIConfig.init(apiConfig)

	if globalIsErasure {
		globalStorageClass, err = storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default],
			globalErasureSetDriveCount)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unable to initialize storage class config: %w", err))
		}
	}

	globalCacheConfig, err = cache.LookupConfig(s[config.CacheSubSys][config.Default])
	if err != nil {
		if globalIsGateway {
			logger.FatalIf(err, "Unable to setup cache")
		} else {
			logger.LogIf(ctx, fmt.Errorf("Unable to setup cache: %w", err))
		}
	}

	if globalCacheConfig.Enabled {
		if cacheEncKey := env.Get(cache.EnvCacheEncryptionMasterKey, ""); cacheEncKey != "" {
			globalCacheKMS, err = crypto.ParseMasterKey(cacheEncKey)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to setup encryption cache: %w", err))
			}
		}
	}

	kmsCfg, err := crypto.LookupConfig(s, globalCertsCADir.Get(), NewGatewayHTTPTransport())
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to setup KMS config: %w", err))
	}

	GlobalKMS, err = crypto.NewKMS(kmsCfg)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to setup KMS with current KMS config: %w", err))
	}

	// Enable auto-encryption if enabled
	globalAutoEncryption = kmsCfg.AutoEncryption

	globalCompressConfig, err = compress.LookupConfig(s[config.CompressionSubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to setup Compression: %w", err))
	}

	globalOpenIDConfig, err = openid.LookupConfig(s[config.IdentityOpenIDSubSys][config.Default],
		NewGatewayHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize OpenID: %w", err))
	}

	opaCfg, err := opa.LookupConfig(s[config.PolicyOPASubSys][config.Default],
		NewGatewayHTTPTransport(), xhttp.DrainBody)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize OPA: %w", err))
	}

	globalOpenIDValidators = getOpenIDValidators(globalOpenIDConfig)
	globalPolicyOPA = opa.New(opaCfg)

	globalLDAPConfig, err = xldap.Lookup(s[config.IdentityLDAPSubSys][config.Default],
		globalRootCAs)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to parse LDAP configuration: %w", err))
	}

	// Load logger targets based on user's configuration
	loggerUserAgent := getUserAgent(getMinioMode())

	loggerCfg, err := logger.LookupConfig(s)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize logger: %w", err))
	}

	for _, l := range loggerCfg.HTTP {
		if l.Enabled {
			// Enable http logging
			logger.AddTarget(
				http.New(http.WithEndpoint(l.Endpoint),
					http.WithAuthToken(l.AuthToken),
					http.WithUserAgent(loggerUserAgent),
					http.WithLogKind(string(logger.All)),
					http.WithTransport(NewGatewayHTTPTransport()),
				),
			)
		}
	}

	for _, l := range loggerCfg.Audit {
		if l.Enabled {
			// Enable http audit logging
			logger.AddAuditTarget(
				http.New(http.WithEndpoint(l.Endpoint),
					http.WithAuthToken(l.AuthToken),
					http.WithUserAgent(loggerUserAgent),
					http.WithLogKind(string(logger.All)),
					http.WithTransport(NewGatewayHTTPTransport()),
				),
			)
		}
	}

	globalConfigTargetList, err = notify.GetNotificationTargets(s, GlobalContext.Done(), NewGatewayHTTPTransport(), false)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}

	globalEnvTargetList, err = notify.GetNotificationTargets(newServerConfig(), GlobalContext.Done(), NewGatewayHTTPTransport(), true)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}
}

// Help - return sub-system level help
type Help struct {
	SubSys          string         `json:"subSys"`
	Description     string         `json:"description"`
	MultipleTargets bool           `json:"multipleTargets"`
	KeysHelp        config.HelpKVS `json:"keysHelp"`
}

// GetHelp - returns help for sub-sys, a key for a sub-system or all the help.
func GetHelp(subSys, key string, envOnly bool) (Help, error) {
	if len(subSys) == 0 {
		return Help{KeysHelp: config.HelpSubSysMap[subSys]}, nil
	}
	subSystemValue := strings.SplitN(subSys, config.SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return Help{}, config.Errorf("invalid number of arguments %s", subSys)
	}

	subSys = subSystemValue[0]

	subSysHelp, ok := config.HelpSubSysMap[""].Lookup(subSys)
	if !ok {
		return Help{}, config.Errorf("unknown sub-system %s", subSys)
	}

	h, ok := config.HelpSubSysMap[subSys]
	if !ok {
		return Help{}, config.Errorf("unknown sub-system %s", subSys)
	}
	if key != "" {
		value, ok := h.Lookup(key)
		if !ok {
			return Help{}, config.Errorf("unknown key %s for sub-system %s",
				key, subSys)
		}
		h = config.HelpKVS{value}
	}

	envHelp := config.HelpKVS{}
	if envOnly {
		// Only for multiple targets, make sure
		// to list the ENV, for regular k/v EnableKey is
		// implicit, for ENVs we cannot make it implicit.
		if subSysHelp.MultipleTargets {
			envK := config.EnvPrefix + strings.Join([]string{
				strings.ToTitle(subSys), strings.ToTitle(madmin.EnableKey),
			}, config.EnvWordDelimiter)
			envHelp = append(envHelp, config.HelpKV{
				Key:         envK,
				Description: fmt.Sprintf("enable %s target, default is 'off'", subSys),
				Optional:    false,
				Type:        "on|off",
			})
		}
		for _, hkv := range h {
			envK := config.EnvPrefix + strings.Join([]string{
				strings.ToTitle(subSys), strings.ToTitle(hkv.Key),
			}, config.EnvWordDelimiter)
			envHelp = append(envHelp, config.HelpKV{
				Key:         envK,
				Description: hkv.Description,
				Optional:    hkv.Optional,
				Type:        hkv.Type,
			})
		}
		h = envHelp
	}

	return Help{
		SubSys:          subSys,
		Description:     subSysHelp.Description,
		MultipleTargets: subSysHelp.MultipleTargets,
		KeysHelp:        h,
	}, nil
}

func newServerConfig() config.Config {
	return config.New()
}

// newSrvConfig - initialize a new server config, saves env parameters if
// found, otherwise use default parameters
func newSrvConfig(objAPI ObjectLayer) error {
	// Initialize server config.
	srvCfg := newServerConfig()

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	// Save config into file.
	return saveServerConfig(GlobalContext, objAPI, globalServerConfig)
}

func getValidConfig(objAPI ObjectLayer) (config.Config, error) {
	return readServerConfig(GlobalContext, objAPI)
}

// loadConfig - loads a new config from disk, overrides params
// from env if found and valid
func loadConfig(objAPI ObjectLayer) error {
	srvCfg, err := getValidConfig(objAPI)
	if err != nil {
		return err
	}

	// Override any values from ENVs.
	lookupConfigs(srvCfg)

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
