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
	"maps"
	"strings"
	"sync"

	"github.com/minio/minio/internal/config/browser"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/api"
	"github.com/minio/minio/internal/config/batch"
	"github.com/minio/minio/internal/config/callhome"
	"github.com/minio/minio/internal/config/compress"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/config/drive"
	"github.com/minio/minio/internal/config/etcd"
	"github.com/minio/minio/internal/config/heal"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	idplugin "github.com/minio/minio/internal/config/identity/plugin"
	xtls "github.com/minio/minio/internal/config/identity/tls"
	"github.com/minio/minio/internal/config/ilm"
	"github.com/minio/minio/internal/config/lambda"
	"github.com/minio/minio/internal/config/notify"
	"github.com/minio/minio/internal/config/policy/opa"
	polplugin "github.com/minio/minio/internal/config/policy/plugin"
	"github.com/minio/minio/internal/config/scanner"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/config/subnet"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
)

func initHelp() {
	kvs := map[string]config.KVS{
		config.EtcdSubSys:           etcd.DefaultKVS,
		config.CompressionSubSys:    compress.DefaultKVS,
		config.IdentityLDAPSubSys:   xldap.DefaultKVS,
		config.IdentityOpenIDSubSys: openid.DefaultKVS,
		config.IdentityTLSSubSys:    xtls.DefaultKVS,
		config.IdentityPluginSubSys: idplugin.DefaultKVS,
		config.PolicyOPASubSys:      opa.DefaultKVS,
		config.PolicyPluginSubSys:   polplugin.DefaultKVS,
		config.SiteSubSys:           config.DefaultSiteKVS,
		config.RegionSubSys:         config.DefaultRegionKVS,
		config.APISubSys:            api.DefaultKVS,
		config.LoggerWebhookSubSys:  logger.DefaultLoggerWebhookKVS,
		config.AuditWebhookSubSys:   logger.DefaultAuditWebhookKVS,
		config.AuditKafkaSubSys:     logger.DefaultAuditKafkaKVS,
		config.ScannerSubSys:        scanner.DefaultKVS,
		config.SubnetSubSys:         subnet.DefaultKVS,
		config.CallhomeSubSys:       callhome.DefaultKVS,
		config.DriveSubSys:          drive.DefaultKVS,
		config.ILMSubSys:            ilm.DefaultKVS,
		config.BatchSubSys:          batch.DefaultKVS,
		config.BrowserSubSys:        browser.DefaultKVS,
	}
	maps.Copy(kvs, notify.DefaultNotificationKVS)
	maps.Copy(kvs, lambda.DefaultLambdaKVS)
	if globalIsErasure {
		kvs[config.StorageClassSubSys] = storageclass.DefaultKVS
		kvs[config.HealSubSys] = heal.DefaultKVS
	}
	config.RegisterDefaultKVS(kvs)

	// Captures help for each sub-system
	helpSubSys := config.HelpKVS{
		config.HelpKV{
			Key:         config.SubnetSubSys,
			Type:        "string",
			Description: "register Enterprise license for the cluster",
			Optional:    true,
		},
		config.HelpKV{
			Key:         config.CallhomeSubSys,
			Type:        "string",
			Description: "enable callhome to MinIO SUBNET",
			Optional:    true,
		},
		config.HelpKV{
			Key:         config.DriveSubSys,
			Description: "enable drive specific settings",
		},
		config.HelpKV{
			Key:         config.SiteSubSys,
			Description: "label the server and its location",
		},
		config.HelpKV{
			Key:         config.APISubSys,
			Description: "manage global HTTP API call specific features, such as throttling, authentication types, etc.",
		},
		config.HelpKV{
			Key:         config.ScannerSubSys,
			Description: "manage namespace scanning for usage calculation, lifecycle, healing and more",
		},
		config.HelpKV{
			Key:         config.BatchSubSys,
			Description: "manage batch job workers and wait times",
		},
		config.HelpKV{
			Key:         config.CompressionSubSys,
			Description: "enable server side compression of objects",
		},
		config.HelpKV{
			Key:             config.IdentityOpenIDSubSys,
			Description:     "enable OpenID SSO support",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:         config.IdentityLDAPSubSys,
			Description: "enable LDAP SSO support",
		},
		config.HelpKV{
			Key:         config.IdentityTLSSubSys,
			Description: "enable X.509 TLS certificate SSO support",
		},
		config.HelpKV{
			Key:         config.IdentityPluginSubSys,
			Description: "enable Identity Plugin via external hook",
		},
		config.HelpKV{
			Key:         config.PolicyPluginSubSys,
			Description: "enable Access Management Plugin for policy enforcement",
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
			Key:             config.AuditKafkaSubSys,
			Description:     "send audit logs to kafka endpoints",
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
		config.HelpKV{
			Key:             config.LambdaWebhookSubSys,
			Description:     "manage remote lambda functions",
			MultipleTargets: true,
		},
		config.HelpKV{
			Key:         config.EtcdSubSys,
			Description: "persist IAM assets externally to etcd",
		},
		config.HelpKV{
			Key:         config.BrowserSubSys,
			Description: "manage Browser HTTP specific features, such as Security headers, etc.",
			Optional:    true,
		},
		config.HelpKV{
			Key:         config.ILMSubSys,
			Description: "manage ILM settings for expiration and transition workers",
			Optional:    true,
		},
	}

	if globalIsErasure {
		helpSubSys = append(helpSubSys, config.HelpKV{
			Key:         config.StorageClassSubSys,
			Description: "define object level redundancy",
		}, config.HelpKV{
			Key:         config.HealSubSys,
			Description: "manage object healing frequency and bitrot verification checks",
		})
	}

	helpMap := map[string]config.HelpKVS{
		"":                          helpSubSys, // Help for all sub-systems.
		config.SiteSubSys:           config.SiteHelp,
		config.RegionSubSys:         config.RegionHelp,
		config.APISubSys:            api.Help,
		config.StorageClassSubSys:   storageclass.Help,
		config.EtcdSubSys:           etcd.Help,
		config.CompressionSubSys:    compress.Help,
		config.HealSubSys:           heal.Help,
		config.BatchSubSys:          batch.Help,
		config.ScannerSubSys:        scanner.Help,
		config.IdentityOpenIDSubSys: openid.Help,
		config.IdentityLDAPSubSys:   xldap.Help,
		config.IdentityTLSSubSys:    xtls.Help,
		config.IdentityPluginSubSys: idplugin.Help,
		config.PolicyOPASubSys:      opa.Help,
		config.PolicyPluginSubSys:   polplugin.Help,
		config.LoggerWebhookSubSys:  logger.Help,
		config.AuditWebhookSubSys:   logger.HelpWebhook,
		config.AuditKafkaSubSys:     logger.HelpKafka,
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
		config.LambdaWebhookSubSys:  lambda.HelpWebhook,
		config.SubnetSubSys:         subnet.HelpSubnet,
		config.CallhomeSubSys:       callhome.HelpCallhome,
		config.DriveSubSys:          drive.HelpDrive,
		config.BrowserSubSys:        browser.Help,
		config.ILMSubSys:            ilm.Help,
	}

	config.RegisterHelpSubSys(helpMap)

	// save top-level help for deprecated sub-systems in a separate map.
	deprecatedHelpKVMap := map[string]config.HelpKV{
		config.RegionSubSys: {
			Key:         config.RegionSubSys,
			Description: "[DEPRECATED - use `site` instead] label the location of the server",
		},
		config.PolicyOPASubSys: {
			Key:         config.PolicyOPASubSys,
			Description: "[DEPRECATED - use `policy_plugin` instead] enable external OPA for policy enforcement",
		},
	}

	config.RegisterHelpDeprecatedSubSys(deprecatedHelpKVMap)
}

var (
	// globalServerConfig server config.
	globalServerConfig   config.Config
	globalServerConfigMu sync.RWMutex
)

func validateSubSysConfig(ctx context.Context, s config.Config, subSys string, objAPI ObjectLayer) error {
	switch subSys {
	case config.SiteSubSys:
		if _, err := config.LookupSite(s[config.SiteSubSys][config.Default], s[config.RegionSubSys][config.Default]); err != nil {
			return err
		}
	case config.APISubSys:
		if _, err := api.LookupConfig(s[config.APISubSys][config.Default]); err != nil {
			return err
		}
	case config.BatchSubSys:
		if _, err := batch.LookupConfig(s[config.BatchSubSys][config.Default]); err != nil {
			return err
		}
	case config.StorageClassSubSys:
		if objAPI == nil {
			return errServerNotInitialized
		}
		for _, setDriveCount := range objAPI.SetDriveCounts() {
			if _, err := storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default], setDriveCount); err != nil {
				return err
			}
		}
	case config.CompressionSubSys:
		if _, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default]); err != nil {
			return err
		}
	case config.HealSubSys:
		if _, err := heal.LookupConfig(s[config.HealSubSys][config.Default]); err != nil {
			return err
		}
	case config.ScannerSubSys:
		if _, err := scanner.LookupConfig(s[config.ScannerSubSys][config.Default]); err != nil {
			return err
		}
	case config.EtcdSubSys:
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
	case config.IdentityOpenIDSubSys:
		if _, err := openid.LookupConfig(s,
			xhttp.WithUserAgent(NewHTTPTransport(), func() string {
				return getUserAgent(getMinioMode())
			}), xhttp.DrainBody, globalSite.Region()); err != nil {
			return err
		}
	case config.IdentityLDAPSubSys:
		cfg, err := xldap.Lookup(s, globalRootCAs)
		if err != nil {
			return err
		}
		if cfg.Enabled() {
			conn, cerr := cfg.LDAP.Connect()
			if cerr != nil {
				return cerr
			}
			conn.Close()
		}
	case config.IdentityTLSSubSys:
		if _, err := xtls.Lookup(s[config.IdentityTLSSubSys][config.Default]); err != nil {
			return err
		}
	case config.IdentityPluginSubSys:
		if _, err := idplugin.LookupConfig(s[config.IdentityPluginSubSys][config.Default],
			NewHTTPTransport(), xhttp.DrainBody, globalSite.Region()); err != nil {
			return err
		}
	case config.SubnetSubSys:
		if _, err := subnet.LookupConfig(s[config.SubnetSubSys][config.Default], nil); err != nil {
			return err
		}
	case config.CallhomeSubSys:
		cfg, err := callhome.LookupConfig(s[config.CallhomeSubSys][config.Default])
		if err != nil {
			return err
		}
		// callhome cannot be enabled if license is not registered yet, throw an error.
		if cfg.Enabled() && !globalSubnetConfig.Registered() {
			return errors.New("Deployment is not registered with SUBNET. Please register the deployment via 'mc license register ALIAS'")
		}
	case config.DriveSubSys:
		if _, err := drive.LookupConfig(s[config.DriveSubSys][config.Default]); err != nil {
			return err
		}
	case config.PolicyOPASubSys:
		// In case legacy OPA config is being set, we treat it as if the
		// AuthZPlugin is being set.
		subSys = config.PolicyPluginSubSys
		fallthrough
	case config.PolicyPluginSubSys:
		if ppargs, err := polplugin.LookupConfig(s, GetDefaultConnSettings(), xhttp.DrainBody); err != nil {
			return err
		} else if ppargs.URL == nil {
			// Check if legacy opa is configured.
			if _, err := opa.LookupConfig(s[config.PolicyOPASubSys][config.Default],
				NewHTTPTransport(), xhttp.DrainBody); err != nil {
				return err
			}
		}
	case config.BrowserSubSys:
		if _, err := browser.LookupConfig(s[config.BrowserSubSys][config.Default]); err != nil {
			return err
		}
	default:
		if config.LoggerSubSystems.Contains(subSys) {
			if err := logger.ValidateSubSysConfig(ctx, s, subSys); err != nil {
				return err
			}
		}
	}

	if config.NotifySubSystems.Contains(subSys) {
		if err := notify.TestSubSysNotificationTargets(ctx, s, subSys, NewHTTPTransport()); err != nil {
			return err
		}
	}

	if config.LambdaSubSystems.Contains(subSys) {
		if err := lambda.TestSubSysLambdaTargets(GlobalContext, s, subSys, NewHTTPTransport()); err != nil {
			return err
		}
	}

	return nil
}

func validateConfig(ctx context.Context, s config.Config, subSys string) error {
	objAPI := newObjectLayerFn()

	// We must have a global lock for this so nobody else modifies env while we do.
	defer env.LockSetEnv()()

	// Disable merging env values with config for validation.
	env.SetEnvOff()

	// Enable env values to validate KMS.
	defer env.SetEnvOn()
	if subSys != "" {
		return validateSubSysConfig(ctx, s, subSys, objAPI)
	}

	// No sub-system passed. Validate all of them.
	for _, ss := range config.SubSystems.ToSlice() {
		if err := validateSubSysConfig(ctx, s, ss, objAPI); err != nil {
			return err
		}
	}

	return nil
}

func lookupConfigs(s config.Config, objAPI ObjectLayer) {
	ctx := GlobalContext

	dnsURL, dnsUser, dnsPass, err := env.LookupEnv(config.EnvDNSWebhook)
	if err != nil {
		configLogIf(ctx, fmt.Errorf("Unable to initialize remote webhook DNS config %w", err))
	}
	if err == nil && dnsURL != "" {
		bootstrapTraceMsg("initialize remote bucket DNS store")
		globalDNSConfig, err = dns.NewOperatorDNS(dnsURL,
			dns.Authentication(dnsUser, dnsPass),
			dns.RootCAs(globalRootCAs))
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to initialize remote webhook DNS config %w", err))
		}
	}

	etcdCfg, err := etcd.LookupConfig(s[config.EtcdSubSys][config.Default], globalRootCAs)
	if err != nil {
		configLogIf(ctx, fmt.Errorf("Unable to initialize etcd config: %w", err))
	}

	if etcdCfg.Enabled {
		bootstrapTraceMsg("initialize etcd store")
		globalEtcdClient, err = etcd.New(etcdCfg)
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to initialize etcd config: %w", err))
		}

		if len(globalDomainNames) != 0 && !globalDomainIPs.IsEmpty() && globalEtcdClient != nil {
			if globalDNSConfig != nil {
				// if global DNS is already configured, indicate with a warning, in case
				// users are confused.
				configLogIf(ctx, fmt.Errorf("DNS store is already configured with %s, etcd is not used for DNS store", globalDNSConfig))
			} else {
				globalDNSConfig, err = dns.NewCoreDNS(etcdCfg.Config,
					dns.DomainNames(globalDomainNames),
					dns.DomainIPs(globalDomainIPs),
					dns.DomainPort(globalMinioPort),
					dns.CoreDNSPath(etcdCfg.CoreDNSPath),
				)
				if err != nil {
					configLogIf(ctx, fmt.Errorf("Unable to initialize DNS config for %s: %w",
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

	siteCfg, err := config.LookupSite(s[config.SiteSubSys][config.Default], s[config.RegionSubSys][config.Default])
	if err != nil {
		configLogIf(ctx, fmt.Errorf("Invalid site configuration: %w", err))
	}
	globalSite.Update(siteCfg)

	globalAutoEncryption = crypto.LookupAutoEncryption() // Enable auto-encryption if enabled
	if globalAutoEncryption && GlobalKMS == nil {
		logger.Fatal(errors.New("no KMS configured"), "MINIO_KMS_AUTO_ENCRYPTION requires a valid KMS configuration")
	}

	transport := NewHTTPTransport()

	bootstrapTraceMsg("initialize the event notification targets")
	globalNotifyTargetList, err = notify.FetchEnabledTargets(GlobalContext, s, transport)
	if err != nil {
		configLogIf(ctx, fmt.Errorf("Unable to initialize notification target(s): %w", err))
	}

	bootstrapTraceMsg("initialize the lambda targets")
	globalLambdaTargetList, err = lambda.FetchEnabledTargets(GlobalContext, s, transport)
	if err != nil {
		configLogIf(ctx, fmt.Errorf("Unable to initialize lambda target(s): %w", err))
	}

	bootstrapTraceMsg("applying the dynamic configuration")
	// Apply dynamic config values
	if err := applyDynamicConfig(ctx, objAPI, s); err != nil {
		configLogIf(ctx, err)
	}
}

func applyDynamicConfigForSubSys(ctx context.Context, objAPI ObjectLayer, s config.Config, subSys string) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	var errs []error
	setDriveCounts := objAPI.SetDriveCounts()
	switch subSys {
	case config.APISubSys:
		apiConfig, err := api.LookupConfig(s[config.APISubSys][config.Default])
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Invalid api configuration: %w", err))
		}

		globalAPIConfig.init(apiConfig, setDriveCounts, objAPI.Legacy())
		setRemoteInstanceTransport(NewHTTPTransportWithTimeout(apiConfig.RemoteTransportDeadline))
	case config.CompressionSubSys:
		cmpCfg, err := compress.LookupConfig(s[config.CompressionSubSys][config.Default])
		if err != nil {
			return fmt.Errorf("Unable to setup Compression: %w", err)
		}
		globalCompressConfigMu.Lock()
		globalCompressConfig = cmpCfg
		globalCompressConfigMu.Unlock()
	case config.HealSubSys:
		healCfg, err := heal.LookupConfig(s[config.HealSubSys][config.Default])
		if err != nil {
			errs = append(errs, fmt.Errorf("Unable to apply heal config: %w", err))
		} else {
			globalHealConfig.Update(healCfg)
		}
	case config.BatchSubSys:
		batchCfg, err := batch.LookupConfig(s[config.BatchSubSys][config.Default])
		if err != nil {
			errs = append(errs, fmt.Errorf("Unable to apply batch config: %w", err))
		} else {
			globalBatchConfig.Update(batchCfg)
		}
	case config.ScannerSubSys:
		scannerCfg, err := scanner.LookupConfig(s[config.ScannerSubSys][config.Default])
		if err != nil {
			errs = append(errs, fmt.Errorf("Unable to apply scanner config: %w", err))
		} else {
			// update dynamic scanner values.
			scannerIdleMode.Store(scannerCfg.IdleMode)
			scannerCycle.Store(scannerCfg.Cycle)
			scannerExcessObjectVersions.Store(scannerCfg.ExcessVersions)
			scannerExcessFolders.Store(scannerCfg.ExcessFolders)
			configLogIf(ctx, scannerSleeper.Update(scannerCfg.Delay, scannerCfg.MaxWait))
		}
	case config.LoggerWebhookSubSys:
		loggerCfg, err := logger.LookupConfigForSubSys(ctx, s, config.LoggerWebhookSubSys)
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to load logger webhook config: %w", err))
		}
		userAgent := getUserAgent(getMinioMode())
		for n, l := range loggerCfg.HTTP {
			if l.Enabled {
				l.LogOnceIf = configLogOnceConsoleIf
				l.UserAgent = userAgent
				l.Transport = NewHTTPTransportWithClientCerts(l.ClientCert, l.ClientKey)
			}
			loggerCfg.HTTP[n] = l
		}
		if errs := logger.UpdateHTTPWebhooks(ctx, loggerCfg.HTTP); len(errs) > 0 {
			configLogIf(ctx, fmt.Errorf("Unable to update logger webhook config: %v", errs))
		}
	case config.AuditWebhookSubSys:
		loggerCfg, err := logger.LookupConfigForSubSys(ctx, s, config.AuditWebhookSubSys)
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to load audit webhook config: %w", err))
		}
		userAgent := getUserAgent(getMinioMode())
		for n, l := range loggerCfg.AuditWebhook {
			if l.Enabled {
				l.LogOnceIf = configLogOnceConsoleIf
				l.UserAgent = userAgent
				l.Transport = NewHTTPTransportWithClientCerts(l.ClientCert, l.ClientKey)
			}
			loggerCfg.AuditWebhook[n] = l
		}

		if errs := logger.UpdateAuditWebhooks(ctx, loggerCfg.AuditWebhook); len(errs) > 0 {
			configLogIf(ctx, fmt.Errorf("Unable to update audit webhook targets: %v", errs))
		}
	case config.AuditKafkaSubSys:
		loggerCfg, err := logger.LookupConfigForSubSys(ctx, s, config.AuditKafkaSubSys)
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to load audit kafka config: %w", err))
		}
		for n, l := range loggerCfg.AuditKafka {
			if l.Enabled {
				if l.TLS.Enable {
					l.TLS.RootCAs = globalRootCAs
				}
				l.LogOnce = configLogOnceIf
				loggerCfg.AuditKafka[n] = l
			}
		}
		if errs := logger.UpdateAuditKafkaTargets(ctx, loggerCfg); len(errs) > 0 {
			configLogIf(ctx, fmt.Errorf("Unable to update audit kafka targets: %v", errs))
		}
	case config.StorageClassSubSys:
		for i, setDriveCount := range setDriveCounts {
			sc, err := storageclass.LookupConfig(s[config.StorageClassSubSys][config.Default], setDriveCount)
			if err != nil {
				configLogIf(ctx, fmt.Errorf("Unable to initialize storage class config: %w", err))
				break
			}
			if i == 0 {
				globalStorageClass.Update(sc)
			}
		}
	case config.SubnetSubSys:
		subnetConfig, err := subnet.LookupConfig(s[config.SubnetSubSys][config.Default], globalRemoteTargetTransport)
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to parse subnet configuration: %w", err))
		} else {
			globalSubnetConfig.Update(subnetConfig, globalIsCICD)
			globalSubnetConfig.ApplyEnv() // update environment settings for Console UI
		}
	case config.CallhomeSubSys:
		callhomeCfg, err := callhome.LookupConfig(s[config.CallhomeSubSys][config.Default])
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to load callhome config: %w", err))
		} else {
			enable := callhomeCfg.Enable && !globalCallhomeConfig.Enabled()
			globalCallhomeConfig.Update(callhomeCfg)
			if enable {
				initCallhome(ctx, objAPI)
			}
		}
	case config.DriveSubSys:
		driveConfig, err := drive.LookupConfig(s[config.DriveSubSys][config.Default])
		if err != nil {
			configLogIf(ctx, fmt.Errorf("Unable to load drive config: %w", err))
		} else {
			if err = globalDriveConfig.Update(driveConfig); err != nil {
				configLogIf(ctx, fmt.Errorf("Unable to update drive config: %v", err))
			}
		}
	case config.BrowserSubSys:
		browserCfg, err := browser.LookupConfig(s[config.BrowserSubSys][config.Default])
		if err != nil {
			errs = append(errs, fmt.Errorf("Unable to apply browser config: %w", err))
		} else {
			globalBrowserConfig.Update(browserCfg)
		}
	case config.ILMSubSys:
		ilmCfg, err := ilm.LookupConfig(s[config.ILMSubSys][config.Default])
		if err != nil {
			errs = append(errs, fmt.Errorf("Unable to apply ilm config: %w", err))
		} else {
			if globalTransitionState != nil {
				globalTransitionState.UpdateWorkers(ilmCfg.TransitionWorkers)
			}
			if globalExpiryState != nil {
				globalExpiryState.ResizeWorkers(ilmCfg.ExpirationWorkers)
			}
			globalILMConfig.update(ilmCfg)
		}
	}
	globalServerConfigMu.Lock()
	defer globalServerConfigMu.Unlock()
	if globalServerConfig != nil {
		globalServerConfig[subSys] = s[subSys]
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// applyDynamicConfig will apply dynamic config values.
// Dynamic systems should be in config.SubSystemsDynamic as well.
func applyDynamicConfig(ctx context.Context, objAPI ObjectLayer, s config.Config) error {
	for subSys := range config.SubSystemsDynamic {
		err := applyDynamicConfigForSubSys(ctx, objAPI, s, subSys)
		if err != nil {
			return err
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
		subSysHelp, ok = config.HelpDeprecatedSubSysMap[subSys]
		if !ok {
			return Help{}, config.Errorf("unknown sub-system %s", subSys)
		}
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

	help := config.HelpKVS{}

	// Only for multiple targets, make sure
	// to list the ENV, for regular k/v EnableKey is
	// implicit, for ENVs we cannot make it implicit.
	if subSysHelp.MultipleTargets {
		key := madmin.EnableKey
		if envOnly {
			key = config.EnvPrefix + strings.ToTitle(subSys) + config.EnvWordDelimiter + strings.ToTitle(madmin.EnableKey)
		}
		help = append(help, config.HelpKV{
			Key:         key,
			Description: fmt.Sprintf("enable %s target, default is 'off'", subSys),
			Optional:    false,
			Type:        "on|off",
		})
	}

	for _, hkv := range h {
		key := hkv.Key
		if envOnly {
			key = config.EnvPrefix + strings.ToTitle(subSys) + config.EnvWordDelimiter + strings.ToTitle(hkv.Key)
		}
		help = append(help, config.HelpKV{
			Key:         key,
			Description: hkv.Description,
			Optional:    hkv.Optional,
			Type:        hkv.Type,
		})
	}

	return Help{
		SubSys:          subSys,
		Description:     subSysHelp.Description,
		MultipleTargets: subSysHelp.MultipleTargets,
		KeysHelp:        help,
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
	return saveServerConfig(GlobalContext, objAPI, srvCfg)
}

func getValidConfig(objAPI ObjectLayer) (config.Config, error) {
	return readServerConfig(GlobalContext, objAPI, nil)
}

// loadConfig - loads a new config from disk, overrides params
// from env if found and valid
// data is optional. If nil it will be loaded from backend.
func loadConfig(objAPI ObjectLayer, data []byte) error {
	bootstrapTraceMsg("load the configuration")
	srvCfg, err := readServerConfig(GlobalContext, objAPI, data)
	if err != nil {
		return err
	}

	bootstrapTraceMsg("lookup the configuration")
	// Override any values from ENVs.
	lookupConfigs(srvCfg, objAPI)

	// hold the mutex lock before a new config is assigned.
	globalServerConfigMu.Lock()
	globalServerConfig = srvCfg
	globalServerConfigMu.Unlock()

	return nil
}
