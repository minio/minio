/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
 *
 */

package config

import (
	"fmt"
	"strings"
)

// Default keys
const (
	Default = "_"
	State   = "state"

	// State values
	StateEnabled  = "on"
	StateDisabled = "off"

	CredentialAccessKey = "accessKey"
	CredentialSecretKey = "secretKey"

	RegionName = "name"

	StorageClassStandard = "standard"
	StorageClassRRS      = "rrs"

	CacheDrives  = "drives"
	CacheExclude = "exclude"
	CacheExpiry  = "expiry"
	CacheQuota   = "quota"

	CompressionExtensions = "extensions"
	CompressionMimeTypes  = "mime_types"

	LoggerHTTPEndpoint = "endpoint"
)

// Top level config constants.
const (
	PolicyOPASubSys       = "policy_opa"
	IdentityOpenIDSubSys  = "identity_openid"
	IdentityLDAPSubSys    = "identity_ldap"
	WormSubSys            = "worm"
	CacheSubSys           = "cache"
	RegionSubSys          = "region"
	CredentialSubSys      = "credential"
	StorageClassSubSys    = "storageclass"
	CompressionSubSys     = "compression"
	KmsVaultSubSys        = "kms_vault"
	LoggerHTTPSubSys      = "logger_http"
	LoggerHTTPAuditSubSys = "logger_http_audit"

	// Add new constants here if you add new fields to config.
)

// Notification config constants.
const (
	NotifyKafkaSubSys    = "notify_kafka"
	NotifyMQTTSubSys     = "notify_mqtt"
	NotifyMySQLSubSys    = "notify_mysql"
	NotifyNATSSubSys     = "notify_nats"
	NotifyNSQSubSys      = "notify_nsq"
	NotifyESSubSys       = "notify_elasticsearch"
	NotifyAMQPSubSys     = "notify_amqp"
	NotifyPostgresSubSys = "notify_postgres"
	NotifyRedisSubSys    = "notify_redis"
	NotifyWebhookSubSys  = "notify_webhook"

	// Add new constants here if you add new fields to config.
)

// SubSystems - various sub systems
var SubSystems = []string{
	PolicyOPASubSys,
	IdentityLDAPSubSys,
	IdentityOpenIDSubSys,
	WormSubSys,
	CacheSubSys,
	RegionSubSys,
	CredentialSubSys,
	StorageClassSubSys,
	CompressionSubSys,
	KmsVaultSubSys,
	LoggerHTTPSubSys,
	NotifyAMQPSubSys,
	NotifyESSubSys,
	NotifyKafkaSubSys,
	NotifyMQTTSubSys,
	NotifyMySQLSubSys,
	NotifyNATSSubSys,
	NotifyNSQSubSys,
	NotifyPostgresSubSys,
	NotifyRedisSubSys,
	NotifyWebhookSubSys,
}

// KVS - is a shorthand for some wrapper functions
// to operate on list of key values.
type KVS map[string]string

// Get - returns the value of a key, if not found returns empty.
func (kvs KVS) Get(key string) string {
	return kvs[key]
}

// Config - MinIO server config structure.
type Config map[string]map[string]KVS

// New - initialize a new server config.
func New() Config {
	srvCfg := make(Config)
	for _, k := range SubSystems {
		srvCfg[k] = map[string]KVS{}
	}
	return srvCfg
}

// Constant separators
const (
	subSystemSeparator = ":"
	kvSeparator        = "="
)

// GetKVS - get kvs from specific subsystem.
func GetKVS(s string, scfg Config) (KVS, error) {
	if len(s) == 0 {
		return nil, fmt.Errorf("invalid number of arguments %s", s)
	}
	inputs := strings.Fields(s)
	if len(inputs) > 1 {
		return nil, fmt.Errorf("invalid number of arguments %s", s)
	}
	subSystemValue := strings.SplitN(inputs[0], subSystemSeparator, 2)
	var found bool
	for _, v := range SubSystems {
		if v == subSystemValue[0] {
			found = true
			break
		}
		found = false
	}
	if !found {
		return nil, fmt.Errorf("unknown sub-system %s", subSystemValue[0])
	}

	if len(subSystemValue) == 2 {
		return scfg[subSystemValue[0]][subSystemValue[1]], nil
	}
	return scfg[subSystemValue[0]][Default], nil
}

// SetKVS - set specific key values per sub-system.
func SetKVS(s string, scfg Config) error {
	inputs := strings.Fields(s)
	if len(inputs) <= 1 {
		return fmt.Errorf("invalid number of arguments %s", s)
	}
	subSystemValue := strings.SplitN(inputs[0], subSystemSeparator, 2)
	var kvs = KVS{}
	for _, v := range inputs[1:] {
		kv := strings.SplitN(v, kvSeparator, 2)
		if len(kv) == 0 {
			continue
		}
		kvs[kv[0]] = kv[1]
	}

	var found bool
	for _, v := range SubSystems {
		if v == subSystemValue[0] {
			found = true
			break
		}
		found = false
	}
	if !found {
		return fmt.Errorf("unknown sub-system %s", subSystemValue[0])
	}
	if len(subSystemValue) == 2 {
		scfg[subSystemValue[0]][subSystemValue[1]] = kvs
	} else {
		scfg[subSystemValue[0]][Default] = kvs
	}
	return nil
}
