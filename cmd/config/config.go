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

// Error config error type
type Error string

func (e Error) Error() string {
	return string(e)
}

// Default keys
const (
	Default = "_"
	State   = "state"

	// State values
	StateOn  = "on"
	StateOff = "off"

	RegionName = "name"
)

// Top level config constants.
const (
	PolicyOPASubSys       = "policy_opa"
	IdentityOpenIDSubSys  = "identity_openid"
	IdentityLDAPSubSys    = "identity_ldap"
	WormSubSys            = "worm"
	CacheSubSys           = "cache"
	RegionSubSys          = "region"
	StorageClassSubSys    = "storageclass"
	CompressionSubSys     = "compression"
	KmsVaultSubSys        = "kms_vault"
	KmsSubSys             = "kms"
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
	StorageClassSubSys,
	CompressionSubSys,
	KmsVaultSubSys,
	LoggerHTTPSubSys,
	LoggerHTTPAuditSubSys,
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

func (kvs KVS) String() string {
	var s strings.Builder
	for k, v := range kvs {
		s.WriteString(k)
		s.WriteString(kvSeparator)
		s.WriteString("\"")
		s.WriteString(v)
		s.WriteString("\"")
		s.WriteString(kvSpaceSeparator)
	}
	return s.String()
}

// Get - returns the value of a key, if not found returns empty.
func (kvs KVS) Get(key string) string {
	return kvs[key]
}

// Config - MinIO server config structure.
type Config map[string]map[string]KVS

func (c Config) String() string {
	var s strings.Builder
	for k, v := range c {
		for target, kv := range v {
			s.WriteString(k)
			if target != Default {
				s.WriteString(subSystemSeparator)
				s.WriteString(target)
			}
			s.WriteString(kvSpaceSeparator)
			s.WriteString(kv.String())
			s.WriteString("\n")
		}
	}
	return s.String()
}

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
	kvSpaceSeparator   = " "
	kvSeparator        = "="
)

// GetKVS - get kvs from specific subsystem.
func GetKVS(s string, scfg Config) (map[string]KVS, error) {
	if len(s) == 0 {
		return nil, Error("input cannot be empty")
	}
	inputs := strings.Fields(s)
	if len(inputs) > 1 {
		return nil, Error(fmt.Sprintf("invalid number of arguments %s", s))
	}
	subSystemValue := strings.SplitN(inputs[0], subSystemSeparator, 2)
	var found bool
	for _, v := range SubSystems {
		if v == subSystemValue[0] {
			found = true
			break
		}
		// Check for sub-prefix only if the input value
		// is only a single value, this rejects invalid
		// inputs if any.
		if strings.HasPrefix(v, subSystemValue[0]) && len(subSystemValue) == 1 {
			found = true
			break
		}
		found = false
	}
	if !found {
		return nil, Error(fmt.Sprintf("unknown sub-system %s", subSystemValue[0]))
	}

	kvs := make(map[string]KVS)
	if len(subSystemValue) == 2 {
		kvs[inputs[0]] = scfg[subSystemValue[0]][subSystemValue[1]]
		return kvs, nil
	}
	for subSys, subSysTgts := range scfg {
		if !strings.HasPrefix(subSys, subSystemValue[0]) {
			continue
		}
		for k, kv := range subSysTgts {
			if k != Default {
				kvs[subSys+subSystemSeparator+k] = kv
			} else {
				kvs[subSys] = kv
			}
		}
	}
	return kvs, nil
}

// DelKVS - delete a specific key.
func DelKVS(s string, scfg Config) error {
	if len(s) == 0 {
		return Error("input arguments cannot be empty")
	}
	inputs := strings.Fields(s)
	if len(inputs) > 1 {
		return Error(fmt.Sprintf("invalid number of arguments %s", s))
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
		return Error(fmt.Sprintf("unknown sub-system %s", subSystemValue[0]))
	}
	if len(subSystemValue) == 2 {
		delete(scfg[subSystemValue[0]], subSystemValue[1])
		return nil
	}
	return Error("default config cannot be removed")
}

// SetKVS - set specific key values per sub-system.
func SetKVS(s string, scfg Config) error {
	if len(s) == 0 {
		return Error("input arguments cannot be empty")
	}
	inputs := strings.Fields(s)
	if len(inputs) <= 1 {
		return Error(fmt.Sprintf("invalid number of arguments %s", s))
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
		return Error(fmt.Sprintf("unknown sub-system %s", subSystemValue[0]))
	}
	if len(subSystemValue) == 2 {
		scfg[subSystemValue[0]][subSystemValue[1]] = kvs
	} else {
		scfg[subSystemValue[0]][Default] = kvs
	}
	return nil
}
