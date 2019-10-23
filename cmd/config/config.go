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

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
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
	Comment = "comment"

	// State values
	StateOn  = "on"
	StateOff = "off"

	RegionName = "name"
	AccessKey  = "access_key"
	SecretKey  = "secret_key"
)

// Top level config constants.
const (
	CredentialsSubSys     = "credentials"
	PolicyOPASubSys       = "policy_opa"
	IdentityOpenIDSubSys  = "identity_openid"
	IdentityLDAPSubSys    = "identity_ldap"
	WormSubSys            = "worm"
	CacheSubSys           = "cache"
	RegionSubSys          = "region"
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

// SubSystems - all supported sub-systems
var SubSystems = set.CreateStringSet([]string{
	CredentialsSubSys,
	WormSubSys,
	RegionSubSys,
	CacheSubSys,
	StorageClassSubSys,
	CompressionSubSys,
	KmsVaultSubSys,
	LoggerHTTPSubSys,
	LoggerHTTPAuditSubSys,
	PolicyOPASubSys,
	IdentityLDAPSubSys,
	IdentityOpenIDSubSys,
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
}...)

// SubSystemsSingleTargets - subsystems which only support single target.
var SubSystemsSingleTargets = set.CreateStringSet([]string{
	CredentialsSubSys,
	WormSubSys,
	RegionSubSys,
	CacheSubSys,
	StorageClassSubSys,
	CompressionSubSys,
	KmsVaultSubSys,
	PolicyOPASubSys,
	IdentityLDAPSubSys,
	IdentityOpenIDSubSys,
}...)

// Constant separators
const (
	SubSystemSeparator = `:`
	KvSeparator        = `=`
	KvSpaceSeparator   = ` `
	KvComment          = `#`
	KvNewline          = "\n"
	KvDoubleQuote      = `"`
	KvSingleQuote      = `'`
)

// KVS - is a shorthand for some wrapper functions
// to operate on list of key values.
type KVS map[string]string

func (kvs KVS) String() string {
	var s strings.Builder
	for k, v := range kvs {
		if k == Comment {
			// Skip the comment, comment will be printed elsewhere.
			continue
		}
		s.WriteString(k)
		s.WriteString(KvSeparator)
		s.WriteString(KvDoubleQuote)
		s.WriteString(v)
		s.WriteString(KvDoubleQuote)
		s.WriteString(KvSpaceSeparator)
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
			c, ok := kv[Comment]
			if ok {
				// For multiple comments split it correctly.
				for _, c1 := range strings.Split(c, KvNewline) {
					if c1 == "" {
						continue
					}
					s.WriteString(color.YellowBold(KvComment))
					s.WriteString(KvSpaceSeparator)
					s.WriteString(color.BlueBold(strings.TrimSpace(c1)))
					s.WriteString(KvNewline)
				}
			}
			s.WriteString(color.CyanBold(k))
			if target != Default {
				s.WriteString(SubSystemSeparator)
				s.WriteString(target)
			}
			s.WriteString(KvSpaceSeparator)
			s.WriteString(kv.String())
			s.WriteString(KvNewline)
		}
	}
	return s.String()
}

// Default KV configs for worm and region
var (
	DefaultCredentialKVS = KVS{
		State:     StateOff,
		Comment:   "This is a default credential configuration",
		AccessKey: auth.DefaultAccessKey,
		SecretKey: auth.DefaultSecretKey,
	}

	DefaultWormKVS = KVS{
		State:   StateOff,
		Comment: "This is a default WORM configuration",
	}

	DefaultRegionKVS = KVS{
		State:      StateOff,
		Comment:    "This is a default Region configuration",
		RegionName: "",
	}
)

// LookupCreds - lookup credentials from config.
func LookupCreds(kv KVS) (auth.Credentials, error) {
	if err := CheckValidKeys(CredentialsSubSys, kv, DefaultCredentialKVS); err != nil {
		return auth.Credentials{}, err
	}
	return auth.CreateCredentials(env.Get(EnvAccessKey, kv.Get(AccessKey)),
		env.Get(EnvSecretKey, kv.Get(SecretKey)))
}

// LookupRegion - get current region.
func LookupRegion(kv KVS) (string, error) {
	if err := CheckValidKeys(RegionSubSys, kv, DefaultRegionKVS); err != nil {
		return "", err
	}
	region := env.Get(EnvRegion, "")
	if region == "" {
		region = env.Get(EnvRegionName, kv.Get(RegionName))
	}
	return region, nil
}

// CheckValidKeys - checks if inputs KVS has the necessary keys,
// returns error if it find extra or superflous keys.
func CheckValidKeys(subSys string, kv KVS, validKVS KVS) error {
	nkv := KVS{}
	for k, v := range kv {
		if _, ok := validKVS[k]; !ok {
			nkv[k] = v
		}
	}
	if len(nkv) > 0 {
		return Error(fmt.Sprintf("found invalid keys (%s) for '%s' sub-system", nkv.String(), subSys))
	}
	return nil
}

// LookupWorm - check if worm is enabled
func LookupWorm(kv KVS) (bool, error) {
	if err := CheckValidKeys(WormSubSys, kv, DefaultWormKVS); err != nil {
		return false, err
	}
	worm := env.Get(EnvWorm, "")
	if worm == "" {
		worm = env.Get(EnvWormState, kv.Get(State))
		if worm == "" {
			return false, nil
		}
	}
	return ParseBool(worm)
}

// New - initialize a new server config.
func New() Config {
	srvCfg := make(Config)
	for _, k := range SubSystems.ToSlice() {
		srvCfg[k] = map[string]KVS{}
	}
	return srvCfg
}

// GetKVS - get kvs from specific subsystem.
func (c Config) GetKVS(s string) (map[string]KVS, error) {
	if len(s) == 0 {
		return nil, Error("input cannot be empty")
	}
	inputs := strings.Fields(s)
	if len(inputs) > 1 {
		return nil, Error(fmt.Sprintf("invalid number of arguments %s", s))
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return nil, Error(fmt.Sprintf("invalid number of arguments %s", s))
	}
	found := SubSystems.Contains(subSystemValue[0])
	if !found {
		// Check for sub-prefix only if the input value
		// is only a single value, this rejects invalid
		// inputs if any.
		found = !SubSystems.FuncMatch(strings.HasPrefix, subSystemValue[0]).IsEmpty() && len(subSystemValue) == 1
	}
	if !found {
		return nil, Error(fmt.Sprintf("unknown sub-system %s", s))
	}

	kvs := make(map[string]KVS)
	var ok bool
	if len(subSystemValue) == 2 {
		if len(subSystemValue[1]) == 0 {
			err := fmt.Sprintf("sub-system target '%s' cannot be empty", s)
			return nil, Error(err)
		}
		kvs[inputs[0]], ok = c[subSystemValue[0]][subSystemValue[1]]
		if !ok {
			err := fmt.Sprintf("sub-system target '%s' doesn't exist, proceed to create a new one", s)
			return nil, Error(err)
		}
		return kvs, nil
	}

	for subSys, subSysTgts := range c {
		if !strings.HasPrefix(subSys, subSystemValue[0]) {
			continue
		}
		for k, kv := range subSysTgts {
			if k != Default {
				kvs[subSys+SubSystemSeparator+k] = kv
			} else {
				kvs[subSys] = kv
			}
		}
	}
	return kvs, nil
}

// DelKVS - delete a specific key.
func (c Config) DelKVS(s string) error {
	if len(s) == 0 {
		return Error("input arguments cannot be empty")
	}
	inputs := strings.Fields(s)
	if len(inputs) > 1 {
		return Error(fmt.Sprintf("invalid number of arguments %s", s))
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return Error(fmt.Sprintf("invalid number of arguments %s", s))
	}
	if !SubSystems.Contains(subSystemValue[0]) {
		return Error(fmt.Sprintf("unknown sub-system %s", s))
	}
	if len(subSystemValue) == 2 {
		if len(subSystemValue[1]) == 0 {
			err := fmt.Sprintf("sub-system target '%s' cannot be empty", s)
			return Error(err)
		}
		delete(c[subSystemValue[0]], subSystemValue[1])
		return nil
	}
	return Error(fmt.Sprintf("default config for '%s' sub-system cannot be removed", s))
}

// This function is needed, to trim off single or double quotes, creeping into the values.
func sanitizeValue(v string) string {
	v = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(v), KvDoubleQuote), KvDoubleQuote)
	return strings.TrimSuffix(strings.TrimPrefix(v, KvSingleQuote), KvSingleQuote)
}

// Clone - clones a config map entirely.
func (c Config) Clone() Config {
	cp := New()
	for subSys, tgtKV := range c {
		cp[subSys] = make(map[string]KVS)
		for tgt, kv := range tgtKV {
			cp[subSys][tgt] = KVS{}
			for k, v := range kv {
				cp[subSys][tgt][k] = v
			}
		}
	}
	return cp
}

// SetKVS - set specific key values per sub-system.
func (c Config) SetKVS(s string, comment string, defaultKVS map[string]KVS) error {
	if len(s) == 0 {
		return Error("input arguments cannot be empty")
	}
	inputs := strings.SplitN(s, KvSpaceSeparator, 2)
	if len(inputs) <= 1 {
		return Error(fmt.Sprintf("invalid number of arguments '%s'", s))
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return Error(fmt.Sprintf("invalid number of arguments %s", s))
	}

	if subSystemValue[0] == CredentialsSubSys {
		return Error(fmt.Sprintf("changing '%s' sub-system values is not allowed, use ENVs instead",
			subSystemValue[0]))
	}

	if !SubSystems.Contains(subSystemValue[0]) {
		return Error(fmt.Sprintf("unknown sub-system %s", s))
	}

	if SubSystemsSingleTargets.Contains(subSystemValue[0]) && len(subSystemValue) == 2 {
		return Error(fmt.Sprintf("sub-system '%s' only supports single target", subSystemValue[0]))
	}

	var kvs = KVS{}
	var prevK string
	for _, v := range strings.Fields(inputs[1]) {
		kv := strings.SplitN(v, KvSeparator, 2)
		if len(kv) == 0 {
			continue
		}
		if len(kv) == 1 && prevK != "" {
			kvs[prevK] = strings.Join([]string{kvs[prevK], sanitizeValue(kv[0])}, KvSpaceSeparator)
			continue
		}
		if len(kv[1]) == 0 {
			err := fmt.Sprintf("value for key '%s' cannot be empty", kv[0])
			return Error(err)
		}
		prevK = kv[0]
		kvs[kv[0]] = sanitizeValue(kv[1])
	}

	if len(subSystemValue) == 2 {
		_, ok := c[subSystemValue[0]][subSystemValue[1]]
		if !ok {
			c[subSystemValue[0]][subSystemValue[1]] = defaultKVS[subSystemValue[0]]
			// Add a comment since its a new target, this comment may be
			// overridden if client supplied it.
			if comment == "" {
				comment = fmt.Sprintf("Settings for sub-system target %s:%s",
					subSystemValue[0], subSystemValue[1])
			}
			c[subSystemValue[0]][subSystemValue[1]][Comment] = comment
		}
	}

	var commentKv bool
	for k, v := range kvs {
		if k == Comment {
			// Set this to true to indicate comment was
			// supplied by client and is going to be preserved.
			commentKv = true
		}
		if len(subSystemValue) == 2 {
			c[subSystemValue[0]][subSystemValue[1]][k] = v
		} else {
			c[subSystemValue[0]][Default][k] = v
		}
	}

	// if client didn't supply the comment try to preserve
	// the comment if any we found while parsing the incoming
	// stream, if not preserve the default.
	if !commentKv && comment != "" {
		if len(subSystemValue) == 2 {
			c[subSystemValue[0]][subSystemValue[1]][Comment] = comment
		} else {
			c[subSystemValue[0]][Default][Comment] = comment
		}
	}
	return nil
}
