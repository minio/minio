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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/api"
	"github.com/minio/minio/internal/config/cache"
	"github.com/minio/minio/internal/config/compress"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/config/etcd"
	"github.com/minio/minio/internal/config/heal"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/config/notify"
	"github.com/minio/minio/internal/config/policy/opa"
	"github.com/minio/minio/internal/config/scanner"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/logger/target/http"
	"github.com/minio/pkg/env"
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
		config.LoggerWebhookSubSys:  logger.DefaultKVS,
		config.AuditWebhookSubSys:   logger.DefaultAuditKVS,
		config.HealSubSys:           heal.DefaultKVS,
		config.ScannerSubSys:        scanner.DefaultKVS,
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
			Description: "[DEPRECATED] enable external OPA for policy enforcement",
		},
		config.HelpKV{
			Key:         config.APISubSys,
			Description: "manage global HTTP API call specific features, such as throttling, authentication types, etc.",
		},
		config.HelpKV{
			Key:         config.HealSubSys,
			Description: "manage object healing frequency and bitrot verification checks",
		},
		config.HelpKV{
			Key:         config.ScannerSubSys,
			Description: "manage namespace scanning for usage calculation, lifecycle, healing and more",
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
		config.HealSubSys:           heal.Help,
		config.ScannerSubSys:        scanner.Help,
		config.IdentityOpenIDSubSys: openid.Help,
		config.IdentityLDAPSubSys:   xldap.Help,
		config.PolicyOPASubSys:      opa.Help,
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

func validateConfig(s config.Config, setDriveCounts []int) error {
	// We must have a global lock for this so nobody else modifies env while we do.
	defer env.LockSetEnv()()

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
		for _, setDriveCount := range setDriveCounts {
			if _, err := storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default], setDriveCount); err != nil {
				return err
			}
		}
	}

	if _, err := cache.LookupConfig(s[config.CacheSubSys][config.Default]); err != nil {
		return err
	}

	compCfg, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default])
	if err != nil {
		return err
	}
	objAPI := newObjectLayerFn()
	if objAPI != nil {
		if compCfg.Enabled && !objAPI.IsCompressionSupported() {
			return fmt.Errorf("Backend does not support compression")
		}
	}

	if _, err = heal.LookupConfig(s[config.HealSubSys][config.Default]); err != nil {
		return err
	}

	if _, err = scanner.LookupConfig(s[config.ScannerSubSys][config.Default]); err != nil {
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

	return notify.TestNotificationTargets(GlobalContext, s, NewGatewayHTTPTransport(), globalNotificationSys.ConfiguredTargetIDs())
}

func lookupConfigs(s config.Config, setDriveCounts []int) {
	ctx := GlobalContext

	var err error
	if !globalActiveCred.IsValid() {
		// Env doesn't seem to be set, we fallback to lookup creds from the config.
		globalActiveCred, err = config.LookupCreds(s[config.CredentialsSubSys][config.Default])
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("Invalid credentials configuration: %w", err))
		}
	}

	if dnsURL, dnsUser, dnsPass, ok := env.LookupEnv(config.EnvDNSWebhook); ok {
		globalDNSConfig, err = dns.NewOperatorDNS(dnsURL,
			dns.Authentication(dnsUser, dnsPass),
			dns.RootCAs(globalRootCAs))
		if err != nil {
			if globalIsGateway {
				logger.FatalIf(err, "Unable to initialize remote webhook DNS config")
			} else {
				logger.LogIf(ctx, fmt.Errorf("Unable to initialize remote webhook DNS config %w", err))
			}
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

		if len(globalDomainNames) != 0 && !globalDomainIPs.IsEmpty() && globalEtcdClient != nil {
			if globalDNSConfig != nil {
				// if global DNS is already configured, indicate with a warning, incase
				// users are confused.
				logger.LogIf(ctx, fmt.Errorf("DNS store is already configured with %s, not using etcd for DNS store", globalDNSConfig))
			} else {
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

	globalAPIConfig.init(apiConfig, setDriveCounts)

	// Initialize remote instance transport once.
	getRemoteInstanceTransportOnce.Do(func() {
		getRemoteInstanceTransport = newGatewayHTTPTransport(apiConfig.RemoteTransportDeadline)
	})

	if globalIsErasure {
		for i, setDriveCount := range setDriveCounts {
			sc, err := storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default], setDriveCount)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to initialize storage class config: %w", err))
				break
			}
			// if we validated all setDriveCounts and it was successful
			// proceed to store the correct storage class globally.
			if i == len(setDriveCounts)-1 {
				globalStorageClass.Update(sc)
			}
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
		if cacheEncKey := env.Get(cache.EnvCacheEncryptionKey, ""); cacheEncKey != "" {
			globalCacheKMS, err = kms.Parse(cacheEncKey)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to setup encryption cache: %w", err))
			}
		}
	}

	globalAutoEncryption = crypto.LookupAutoEncryption() // Enable auto-encryption if enabled
	if globalAutoEncryption && GlobalKMS == nil {
		logger.Fatal(errors.New("no KMS configured"), "MINIO_KMS_AUTO_ENCRYPTION requires a valid KMS configuration")
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

	for k, l := range loggerCfg.HTTP {
		if l.Enabled {
			// Enable http logging
			if err = logger.AddTarget(
				http.New(
					http.WithTargetName(k),
					http.WithEndpoint(l.Endpoint),
					http.WithAuthToken(l.AuthToken),
					http.WithUserAgent(loggerUserAgent),
					http.WithLogKind(string(logger.All)),
					http.WithTransport(NewGatewayHTTPTransport()),
				),
			); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to initialize console HTTP target: %w", err))
			}
		}
	}

	for k, l := range loggerCfg.Audit {
		if l.Enabled {
			// Enable http audit logging
			if err = logger.AddAuditTarget(
				http.New(
					http.WithTargetName(k),
					http.WithEndpoint(l.Endpoint),
					http.WithAuthToken(l.AuthToken),
					http.WithUserAgent(loggerUserAgent),
					http.WithLogKind(string(logger.All)),
					http.WithTransport(NewGatewayHTTPTransportWithClientCerts(l.ClientCert, l.ClientKey)),
				),
			); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to initialize audit HTTP target: %w", err))
			}
		}
	}

	globalConfigTargetList, err = notify.GetNotificationTargets(GlobalContext, s, NewGatewayHTTPTransport(), false)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}

	globalEnvTargetList, err = notify.GetNotificationTargets(GlobalContext, newServerConfig(), NewGatewayHTTPTransport(), true)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}

	// Apply dynamic config values
	logger.LogIf(ctx, applyDynamicConfig(ctx, newObjectLayerFn(), s))
}

// applyDynamicConfig will apply dynamic config values.
// Dynamic systems should be in config.SubSystemsDynamic as well.
func applyDynamicConfig(ctx context.Context, objAPI ObjectLayer, s config.Config) error {
	if objAPI == nil {
		return nil
	}

	// Read all dynamic configs.
	// API
	apiConfig, err := api.LookupConfig(s[config.APISubSys][config.Default])
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Invalid api configuration: %w", err))
	}

	// Compression
	cmpCfg, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default])
	if err != nil {
		return fmt.Errorf("Unable to setup Compression: %w", err)
	}

	// Validate if the object layer supports compression.
	if cmpCfg.Enabled && !objAPI.IsCompressionSupported() {
		return fmt.Errorf("Backend does not support compression")
	}

	// Heal
	healCfg, err := heal.LookupConfig(s[config.HealSubSys][config.Default])
	if err != nil {
		return fmt.Errorf("Unable to apply heal config: %w", err)
	}

	// Scanner
	scannerCfg, err := scanner.LookupConfig(s[config.ScannerSubSys][config.Default])
	if err != nil {
		return fmt.Errorf("Unable to apply scanner config: %w", err)
	}

	// Apply configurations.
	// We should not fail after this.
	globalAPIConfig.init(apiConfig, objAPI.SetDriveCounts())

	globalCompressConfigMu.Lock()
	globalCompressConfig = cmpCfg
	globalCompressConfigMu.Unlock()

	globalHealConfigMu.Lock()
	globalHealConfig = healCfg
	globalHealConfigMu.Unlock()

	// update dynamic scanner values.
	scannerCycle.Update(scannerCfg.Cycle)
	logger.LogIf(ctx, scannerSleeper.Update(scannerCfg.Delay, scannerCfg.MaxWait))

	// Update all dynamic config values in memory.
	globalServerConfigMu.Lock()
	defer globalServerConfigMu.Unlock()
	if globalServerConfig != nil {
		for k := range config.SubSystemsDynamic {
			globalServerConfig[k] = s[k]
		}
	}
	return nil
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
	lookupConfigs(srvCfg, objAPI.SetDriveCounts())

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
