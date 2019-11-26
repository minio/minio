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
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/madmin"
)

// Error config error type
type Error string

// Errorf - formats according to a format specifier and returns
// the string as a value that satisfies error of type config.Error
func Errorf(format string, a ...interface{}) error {
	return Error(fmt.Sprintf(format, a...))
}

func (e Error) Error() string {
	return string(e)
}

// Default keys
const (
	Default = madmin.Default
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
	CredentialsSubSys    = "credentials"
	PolicyOPASubSys      = "policy_opa"
	IdentityOpenIDSubSys = "identity_openid"
	IdentityLDAPSubSys   = "identity_ldap"
	CacheSubSys          = "cache"
	RegionSubSys         = "region"
	EtcdSubSys           = "etcd"
	StorageClassSubSys   = "storageclass"
	CompressionSubSys    = "compression"
	KmsVaultSubSys       = "kms_vault"
	LoggerWebhookSubSys  = "logger_webhook"
	AuditWebhookSubSys   = "audit_webhook"

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
	RegionSubSys,
	EtcdSubSys,
	CacheSubSys,
	StorageClassSubSys,
	CompressionSubSys,
	KmsVaultSubSys,
	LoggerWebhookSubSys,
	AuditWebhookSubSys,
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
	RegionSubSys,
	EtcdSubSys,
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
	SubSystemSeparator = madmin.SubSystemSeparator
	KvSeparator        = madmin.KvSeparator
	KvSpaceSeparator   = madmin.KvSpaceSeparator
	KvComment          = `#`
	KvNewline          = madmin.KvNewline
	KvDoubleQuote      = madmin.KvDoubleQuote
	KvSingleQuote      = madmin.KvSingleQuote

	// Env prefix used for all envs in MinIO
	EnvPrefix        = "MINIO_"
	EnvWordDelimiter = `_`
)

// DefaultKVS - default kvs for all sub-systems
var DefaultKVS map[string]KVS

// RegisterDefaultKVS - this function saves input kvsMap
// globally, this should be called only once preferably
// during `init()`.
func RegisterDefaultKVS(kvsMap map[string]KVS) {
	DefaultKVS = map[string]KVS{}
	for subSys, kvs := range kvsMap {
		DefaultKVS[subSys] = kvs
	}
}

// HelpSubSysMap - help for all individual KVS for each sub-systems
// also carries a special empty sub-system which dumps
// help for each sub-system key.
var HelpSubSysMap map[string]HelpKVS

// RegisterHelpSubSys - this function saves
// input help KVS for each sub-system globally,
// this function should be called only once
// preferably in during `init()`.
func RegisterHelpSubSys(helpKVSMap map[string]HelpKVS) {
	HelpSubSysMap = map[string]HelpKVS{}
	for subSys, hkvs := range helpKVSMap {
		HelpSubSysMap[subSys] = hkvs
	}
}

// KV - is a shorthand of each key value.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// KVS - is a shorthand for some wrapper functions
// to operate on list of key values.
type KVS []KV

// Empty - return if kv is empty
func (kvs KVS) Empty() bool {
	return len(kvs) == 0
}

func (kvs KVS) String() string {
	var s strings.Builder
	for _, kv := range kvs {
		// Do not need to print if state is on
		if kv.Key == State && kv.Value == StateOn {
			continue
		}
		s.WriteString(kv.Key)
		s.WriteString(KvSeparator)
		s.WriteString(KvDoubleQuote)
		s.WriteString(kv.Value)
		s.WriteString(KvDoubleQuote)
		s.WriteString(KvSpaceSeparator)
	}
	return s.String()
}

// Get - returns the value of a key, if not found returns empty.
func (kvs KVS) Get(key string) string {
	v, ok := kvs.Lookup(key)
	if ok {
		return v
	}
	return ""
}

// Lookup - lookup a key in a list of KVS
func (kvs KVS) Lookup(key string) (string, bool) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return "", false
}

// Config - MinIO server config structure.
type Config map[string]map[string]KVS

func (c Config) String() string {
	var s strings.Builder
	for k, v := range c {
		for target, kv := range v {
			s.WriteString(k)
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

// DelFrom - deletes all keys in the input reader.
func (c Config) DelFrom(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// Skip any empty lines, or comment like characters
		if scanner.Text() == "" || strings.HasPrefix(scanner.Text(), KvComment) {
			continue
		}
		if err := c.DelKVS(scanner.Text()); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// ReadFrom - implements io.ReaderFrom interface
func (c Config) ReadFrom(r io.Reader) (int64, error) {
	var n int
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// Skip any empty lines, or comment like characters
		if scanner.Text() == "" || strings.HasPrefix(scanner.Text(), KvComment) {
			continue
		}
		if err := c.SetKVS(scanner.Text(), DefaultKVS); err != nil {
			return 0, err
		}
		n += len(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return int64(n), nil
}

type configWriteTo struct {
	Config
	filterByKey string
}

// NewConfigWriteTo - returns a struct which
// allows for serializing the config/kv struct
// to a io.WriterTo
func NewConfigWriteTo(cfg Config, key string) io.WriterTo {
	return &configWriteTo{Config: cfg, filterByKey: key}
}

// WriteTo - implements io.WriterTo interface implementation for config.
func (c *configWriteTo) WriteTo(w io.Writer) (int64, error) {
	if c.filterByKey == "" {
		n, err := w.Write([]byte(c.String()))
		return int64(n), err
	}
	kvs, err := c.GetKVS(c.filterByKey, DefaultKVS)
	if err != nil {
		return 0, err
	}
	var n int
	for k, kv := range kvs {
		m1, _ := w.Write([]byte(k))
		m2, _ := w.Write([]byte(KvSpaceSeparator))
		m3, _ := w.Write([]byte(kv.String()))
		if len(kvs) > 1 {
			m4, _ := w.Write([]byte(KvNewline))
			n += m1 + m2 + m3 + m4
		} else {
			n += m1 + m2 + m3
		}
	}
	return int64(n), nil
}

// Default KV configs for worm and region
var (
	DefaultCredentialKVS = KVS{
		KV{
			Key:   State,
			Value: StateOff,
		},
		KV{
			Key:   AccessKey,
			Value: auth.DefaultAccessKey,
		},
		KV{
			Key:   SecretKey,
			Value: auth.DefaultSecretKey,
		},
	}

	DefaultRegionKVS = KVS{
		KV{
			Key:   State,
			Value: StateOff,
		},
		KV{
			Key:   RegionName,
			Value: "",
		},
	}
)

// LookupCreds - lookup credentials from config.
func LookupCreds(kv KVS) (auth.Credentials, error) {
	if err := CheckValidKeys(CredentialsSubSys, kv, DefaultCredentialKVS); err != nil {
		return auth.Credentials{}, err
	}
	accessKey := env.Get(EnvAccessKey, kv.Get(AccessKey))
	secretKey := env.Get(EnvSecretKey, kv.Get(SecretKey))
	if accessKey == "" && secretKey == "" {
		accessKey = auth.DefaultAccessKey
		secretKey = auth.DefaultSecretKey
	}
	return auth.CreateCredentials(accessKey, secretKey)
}

var validRegionRegex = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9-_-]+$")

// LookupRegion - get current region.
func LookupRegion(kv KVS) (string, error) {
	if err := CheckValidKeys(RegionSubSys, kv, DefaultRegionKVS); err != nil {
		return "", err
	}
	region := env.Get(EnvRegion, "")
	if region == "" {
		region = env.Get(EnvRegionName, kv.Get(RegionName))
	}
	if region != "" {
		if validRegionRegex.MatchString(region) {
			return region, nil
		}
		return "", Errorf("region '%s' is invalid, expected simple characters such as [us-east-1, myregion...]",
			region)
	}
	return "", nil
}

// CheckValidKeys - checks if inputs KVS has the necessary keys,
// returns error if it find extra or superflous keys.
func CheckValidKeys(subSys string, kv KVS, validKVS KVS) error {
	nkv := KVS{}
	for _, kv := range kv {
		if _, ok := validKVS.Lookup(kv.Key); !ok {
			nkv = append(nkv, kv)
		}
	}
	if len(nkv) > 0 {
		return Errorf("found invalid keys (%s) for '%s' sub-system", nkv.String(), subSys)
	}
	return nil
}

// LookupWorm - check if worm is enabled
func LookupWorm() (bool, error) {
	return ParseBool(env.Get(EnvWorm, StateOff))
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
func (c Config) GetKVS(s string, defaultKVS map[string]KVS) (map[string]KVS, error) {
	if len(s) == 0 {
		return nil, Error("input cannot be empty")
	}
	inputs := strings.Fields(s)
	if len(inputs) > 1 {
		return nil, Errorf("invalid number of arguments %s", s)
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return nil, Errorf("invalid number of arguments %s", s)
	}
	found := SubSystems.Contains(subSystemValue[0])
	if !found {
		// Check for sub-prefix only if the input value is only a
		// single value, this rejects invalid inputs if any.
		found = !SubSystems.FuncMatch(strings.HasPrefix, subSystemValue[0]).IsEmpty() && len(subSystemValue) == 1
	}
	if !found {
		return nil, Errorf("unknown sub-system %s", s)
	}

	kvs := make(map[string]KVS)
	var ok bool
	subSysPrefix := subSystemValue[0]
	if len(subSystemValue) == 2 {
		if len(subSystemValue[1]) == 0 {
			return nil, Errorf("sub-system target '%s' cannot be empty", s)
		}
		kvs[inputs[0]], ok = c[subSysPrefix][subSystemValue[1]]
		if !ok {
			return nil, Errorf("sub-system target '%s' doesn't exist", s)
		}
	} else {
		for subSys, subSysTgts := range c {
			if !strings.HasPrefix(subSys, subSysPrefix) {
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
	}
	if len(kvs) == 0 {
		kvs[subSysPrefix] = defaultKVS[subSysPrefix]
		return kvs, nil
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
		return Errorf("invalid number of arguments %s", s)
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return Errorf("invalid number of arguments %s", s)
	}
	if !SubSystems.Contains(subSystemValue[0]) {
		return Errorf("unknown sub-system %s", s)
	}
	tgt := Default
	subSys := subSystemValue[0]
	if len(subSystemValue) == 2 {
		if len(subSystemValue[1]) == 0 {
			return Errorf("sub-system target '%s' cannot be empty", s)
		}
		tgt = subSystemValue[1]
	}
	_, ok := c[subSys][tgt]
	if !ok {
		return Errorf("sub-system %s already deleted", s)
	}
	delete(c[subSys], tgt)
	return nil
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
			cp[subSys][tgt] = append(cp[subSys][tgt], kv...)
		}
	}
	return cp
}

// SetKVS - set specific key values per sub-system.
func (c Config) SetKVS(s string, defaultKVS map[string]KVS) error {
	if len(s) == 0 {
		return Error("input arguments cannot be empty")
	}
	inputs := strings.SplitN(s, KvSpaceSeparator, 2)
	if len(inputs) <= 1 {
		return Errorf("invalid number of arguments '%s'", s)
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return Errorf("invalid number of arguments %s", s)
	}

	if !SubSystems.Contains(subSystemValue[0]) {
		return Errorf("unknown sub-system %s", s)
	}

	if SubSystemsSingleTargets.Contains(subSystemValue[0]) && len(subSystemValue) == 2 {
		return Errorf("sub-system '%s' only supports single target", subSystemValue[0])
	}

	var kvs = KVS{}
	var prevK string
	for _, v := range strings.Fields(inputs[1]) {
		kv := strings.SplitN(v, KvSeparator, 2)
		if len(kv) == 0 {
			continue
		}
		if len(kv) == 1 && prevK != "" {
			kvs = append(kvs, KV{
				Key:   prevK,
				Value: strings.Join([]string{kvs.Get(prevK), sanitizeValue(kv[0])}, KvSpaceSeparator),
			})
			continue
		}
		if len(kv) == 1 {
			return Errorf("key '%s', cannot have empty value", kv[0])
		}
		prevK = kv[0]
		kvs = append(kvs, KV{
			Key:   kv[0],
			Value: sanitizeValue(kv[1]),
		})
	}

	tgt := Default
	subSys := subSystemValue[0]
	if len(subSystemValue) == 2 {
		tgt = subSystemValue[1]
	}

	// Save client sent kvs
	c[subSys][tgt] = kvs

	_, ok := c[subSys][tgt].Lookup(State)
	if !ok {
		// implicit state "on" if not specified.
		c[subSys][tgt] = append(c[subSys][tgt], KV{
			Key:   State,
			Value: StateOn,
		})
	}
	for _, kv := range defaultKVS[subSys] {
		_, ok := c[subSys][tgt].Lookup(kv.Key)
		if !ok {
			c[subSys][tgt] = append(c[subSys][tgt], kv)
		}
	}
	return nil
}
