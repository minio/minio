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

package config

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/env"
)

// Error config error type
type Error struct {
	Err string
}

// Errorf - formats according to a format specifier and returns
// the string as a value that satisfies error of type config.Error
func Errorf(format string, a ...interface{}) error {
	return Error{Err: fmt.Sprintf(format, a...)}
}

func (e Error) Error() string {
	return e.Err
}

// Default keys
const (
	Default = madmin.Default
	Enable  = madmin.EnableKey
	Comment = madmin.CommentKey

	// Enable values
	EnableOn  = madmin.EnableOn
	EnableOff = madmin.EnableOff

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
	StorageClassSubSys   = "storage_class"
	APISubSys            = "api"
	CompressionSubSys    = "compression"
	LoggerWebhookSubSys  = "logger_webhook"
	AuditWebhookSubSys   = "audit_webhook"
	HealSubSys           = "heal"
	ScannerSubSys        = "scanner"
	CrawlerSubSys        = "crawler"

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
var SubSystems = set.CreateStringSet(
	CredentialsSubSys,
	RegionSubSys,
	EtcdSubSys,
	CacheSubSys,
	APISubSys,
	StorageClassSubSys,
	CompressionSubSys,
	LoggerWebhookSubSys,
	AuditWebhookSubSys,
	PolicyOPASubSys,
	IdentityLDAPSubSys,
	IdentityOpenIDSubSys,
	ScannerSubSys,
	HealSubSys,
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
)

// SubSystemsDynamic - all sub-systems that have dynamic config.
var SubSystemsDynamic = set.CreateStringSet(
	APISubSys,
	CompressionSubSys,
	ScannerSubSys,
	HealSubSys,
)

// SubSystemsSingleTargets - subsystems which only support single target.
var SubSystemsSingleTargets = set.CreateStringSet([]string{
	CredentialsSubSys,
	RegionSubSys,
	EtcdSubSys,
	CacheSubSys,
	APISubSys,
	StorageClassSubSys,
	CompressionSubSys,
	PolicyOPASubSys,
	IdentityLDAPSubSys,
	IdentityOpenIDSubSys,
	HealSubSys,
	ScannerSubSys,
}...)

// Constant separators
const (
	SubSystemSeparator = madmin.SubSystemSeparator
	KvSeparator        = madmin.KvSeparator
	KvSpaceSeparator   = madmin.KvSpaceSeparator
	KvComment          = madmin.KvComment
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

// Keys returns the list of keys for the current KVS
func (kvs KVS) Keys() []string {
	var keys = make([]string, len(kvs))
	var foundComment bool
	for i := range kvs {
		if kvs[i].Key == madmin.CommentKey {
			foundComment = true
		}
		keys[i] = kvs[i].Key
	}
	// Comment KV not found, add it explicitly.
	if !foundComment {
		keys = append(keys, madmin.CommentKey)
	}
	return keys
}

func (kvs KVS) String() string {
	var s strings.Builder
	for _, kv := range kvs {
		// Do not need to print if state is on
		if kv.Key == Enable && kv.Value == EnableOn {
			continue
		}
		s.WriteString(kv.Key)
		s.WriteString(KvSeparator)
		spc := madmin.HasSpace(kv.Value)
		if spc {
			s.WriteString(KvDoubleQuote)
		}
		s.WriteString(kv.Value)
		if spc {
			s.WriteString(KvDoubleQuote)
		}
		s.WriteString(KvSpaceSeparator)
	}
	return s.String()
}

// Set sets a value, if not sets a default value.
func (kvs *KVS) Set(key, value string) {
	for i, kv := range *kvs {
		if kv.Key == key {
			(*kvs)[i] = KV{
				Key:   key,
				Value: value,
			}
			return
		}
	}
	*kvs = append(*kvs, KV{
		Key:   key,
		Value: value,
	})
}

// Get - returns the value of a key, if not found returns empty.
func (kvs KVS) Get(key string) string {
	v, ok := kvs.Lookup(key)
	if ok {
		return v
	}
	return ""
}

// Delete - deletes the key if present from the KV list.
func (kvs *KVS) Delete(key string) {
	for i, kv := range *kvs {
		if kv.Key == key {
			*kvs = append((*kvs)[:i], (*kvs)[i+1:]...)
			return
		}
	}
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

// DelFrom - deletes all keys in the input reader.
func (c Config) DelFrom(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// Skip any empty lines, or comment like characters
		text := scanner.Text()
		if text == "" || strings.HasPrefix(text, KvComment) {
			continue
		}
		if err := c.DelKVS(text); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// ReadConfig - read content from input and write into c.
// Returns whether all parameters were dynamic.
func (c Config) ReadConfig(r io.Reader) (dynOnly bool, err error) {
	var n int
	scanner := bufio.NewScanner(r)
	dynOnly = true
	for scanner.Scan() {
		// Skip any empty lines, or comment like characters
		text := scanner.Text()
		if text == "" || strings.HasPrefix(text, KvComment) {
			continue
		}
		dynamic, err := c.SetKVS(text, DefaultKVS)
		if err != nil {
			return false, err
		}
		dynOnly = dynOnly && dynamic
		n += len(text)
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return dynOnly, nil
}

// RedactSensitiveInfo - removes sensitive information
// like urls and credentials from the configuration
func (c Config) RedactSensitiveInfo() Config {
	nc := c.Clone()

	for configName, configVals := range nc {
		for _, helpKV := range HelpSubSysMap[configName] {
			if helpKV.Sensitive {
				for name, kvs := range configVals {
					for i := range kvs {
						if kvs[i].Key == helpKV.Key && len(kvs[i].Value) > 0 {
							kvs[i].Value = "*redacted*"
						}
					}
					configVals[name] = kvs
				}
			}
		}
		nc[configName] = configVals
	}

	return nc
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
	kvsTargets, err := c.GetKVS(c.filterByKey, DefaultKVS)
	if err != nil {
		return 0, err
	}
	var n int
	for _, target := range kvsTargets {
		m1, _ := w.Write([]byte(target.SubSystem))
		m2, _ := w.Write([]byte(KvSpaceSeparator))
		m3, _ := w.Write([]byte(target.KVS.String()))
		if len(kvsTargets) > 1 {
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
	accessKey := kv.Get(AccessKey)
	secretKey := kv.Get(SecretKey)
	if accessKey == "" || secretKey == "" {
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
		return "", Errorf(
			"region '%s' is invalid, expected simple characters such as [us-east-1, myregion...]",
			region)
	}
	return "", nil
}

// CheckValidKeys - checks if inputs KVS has the necessary keys,
// returns error if it find extra or superflous keys.
func CheckValidKeys(subSys string, kv KVS, validKVS KVS) error {
	nkv := KVS{}
	for _, kv := range kv {
		// Comment is a valid key, its also fully optional
		// ignore it since it is a valid key for all
		// sub-systems.
		if kv.Key == Comment {
			continue
		}
		if _, ok := validKVS.Lookup(kv.Key); !ok {
			nkv = append(nkv, kv)
		}
	}
	if len(nkv) > 0 {
		return Errorf(
			"found invalid keys (%s) for '%s' sub-system, use 'mc admin config reset myminio %s' to fix invalid keys", nkv.String(), subSys, subSys)
	}
	return nil
}

// LookupWorm - check if worm is enabled
func LookupWorm() (bool, error) {
	return ParseBool(env.Get(EnvWorm, EnableOff))
}

// Carries all the renamed sub-systems from their
// previously known names
var renamedSubsys = map[string]string{
	CrawlerSubSys: ScannerSubSys,
	// Add future sub-system renames
}

// Merge - merges a new config with all the
// missing values for default configs,
// returns a config.
func (c Config) Merge() Config {
	cp := New()
	for subSys, tgtKV := range c {
		for tgt := range tgtKV {
			ckvs := c[subSys][tgt]
			for _, kv := range cp[subSys][Default] {
				_, ok := c[subSys][tgt].Lookup(kv.Key)
				if !ok {
					ckvs.Set(kv.Key, kv.Value)
				}
			}
			if _, ok := cp[subSys]; !ok {
				rnSubSys, ok := renamedSubsys[subSys]
				if !ok {
					// A config subsystem was removed or server was downgraded.
					continue
				}
				// Copy over settings from previous sub-system
				// to newly renamed sub-system
				for _, kv := range cp[rnSubSys][Default] {
					_, ok := c[subSys][tgt].Lookup(kv.Key)
					if !ok {
						ckvs.Set(kv.Key, kv.Value)
					}
				}
				subSys = rnSubSys
			}
			cp[subSys][tgt] = ckvs
		}
	}
	return cp
}

// New - initialize a new server config.
func New() Config {
	srvCfg := make(Config)
	for _, k := range SubSystems.ToSlice() {
		srvCfg[k] = map[string]KVS{}
		srvCfg[k][Default] = DefaultKVS[k]
	}
	return srvCfg
}

// Target signifies an individual target
type Target struct {
	SubSystem string
	KVS       KVS
}

// Targets sub-system targets
type Targets []Target

// GetKVS - get kvs from specific subsystem.
func (c Config) GetKVS(s string, defaultKVS map[string]KVS) (Targets, error) {
	if len(s) == 0 {
		return nil, Errorf("input cannot be empty")
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

	targets := Targets{}
	subSysPrefix := subSystemValue[0]
	if len(subSystemValue) == 2 {
		if len(subSystemValue[1]) == 0 {
			return nil, Errorf("sub-system target '%s' cannot be empty", s)
		}
		kvs, ok := c[subSysPrefix][subSystemValue[1]]
		if !ok {
			return nil, Errorf("sub-system target '%s' doesn't exist", s)
		}
		for _, kv := range defaultKVS[subSysPrefix] {
			_, ok = kvs.Lookup(kv.Key)
			if !ok {
				kvs.Set(kv.Key, kv.Value)
			}
		}
		targets = append(targets, Target{
			SubSystem: inputs[0],
			KVS:       kvs,
		})
	} else {
		hkvs := HelpSubSysMap[""]
		// Use help for sub-system to preserve the order.
		for _, hkv := range hkvs {
			if !strings.HasPrefix(hkv.Key, subSysPrefix) {
				continue
			}
			if c[hkv.Key][Default].Empty() {
				targets = append(targets, Target{
					SubSystem: hkv.Key,
					KVS:       defaultKVS[hkv.Key],
				})
			}
			for k, kvs := range c[hkv.Key] {
				for _, dkv := range defaultKVS[hkv.Key] {
					_, ok := kvs.Lookup(dkv.Key)
					if !ok {
						kvs.Set(dkv.Key, dkv.Value)
					}
				}
				if k != Default {
					targets = append(targets, Target{
						SubSystem: hkv.Key + SubSystemSeparator + k,
						KVS:       kvs,
					})
				} else {
					targets = append(targets, Target{
						SubSystem: hkv.Key,
						KVS:       kvs,
					})
				}
			}
		}
	}
	return targets, nil
}

// DelKVS - delete a specific key.
func (c Config) DelKVS(s string) error {
	if len(s) == 0 {
		return Errorf("input arguments cannot be empty")
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
		// Unknown sub-system found try to remove it anyways.
		delete(c, subSystemValue[0])
		return nil
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
func (c Config) SetKVS(s string, defaultKVS map[string]KVS) (dynamic bool, err error) {
	if len(s) == 0 {
		return false, Errorf("input arguments cannot be empty")
	}
	inputs := strings.SplitN(s, KvSpaceSeparator, 2)
	if len(inputs) <= 1 {
		return false, Errorf("invalid number of arguments '%s'", s)
	}
	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return false, Errorf("invalid number of arguments %s", s)
	}

	if !SubSystems.Contains(subSystemValue[0]) {
		return false, Errorf("unknown sub-system %s", s)
	}

	if SubSystemsSingleTargets.Contains(subSystemValue[0]) && len(subSystemValue) == 2 {
		return false, Errorf("sub-system '%s' only supports single target", subSystemValue[0])
	}
	dynamic = SubSystemsDynamic.Contains(subSystemValue[0])

	tgt := Default
	subSys := subSystemValue[0]
	if len(subSystemValue) == 2 {
		tgt = subSystemValue[1]
	}

	fields := madmin.KvFields(inputs[1], defaultKVS[subSys].Keys())
	if len(fields) == 0 {
		return false, Errorf("sub-system '%s' cannot have empty keys", subSys)
	}

	var kvs = KVS{}
	var prevK string
	for _, v := range fields {
		kv := strings.SplitN(v, KvSeparator, 2)
		if len(kv) == 0 {
			continue
		}
		if len(kv) == 1 && prevK != "" {
			value := strings.Join([]string{
				kvs.Get(prevK),
				madmin.SanitizeValue(kv[0]),
			}, KvSpaceSeparator)
			kvs.Set(prevK, value)
			continue
		}
		if len(kv) == 2 {
			prevK = kv[0]
			kvs.Set(prevK, madmin.SanitizeValue(kv[1]))
			continue
		}
		return false, Errorf("key '%s', cannot have empty value", kv[0])
	}

	_, ok := kvs.Lookup(Enable)
	// Check if state is required
	_, enableRequired := defaultKVS[subSys].Lookup(Enable)
	if !ok && enableRequired {
		// implicit state "on" if not specified.
		kvs.Set(Enable, EnableOn)
	}

	currKVS, ok := c[subSys][tgt]
	if !ok {
		currKVS = defaultKVS[subSys]
	} else {
		for _, kv := range defaultKVS[subSys] {
			if _, ok = currKVS.Lookup(kv.Key); !ok {
				currKVS.Set(kv.Key, kv.Value)
			}
		}
	}

	for _, kv := range kvs {
		if kv.Key == Comment {
			// Skip comment and add it later.
			continue
		}
		currKVS.Set(kv.Key, kv.Value)
	}

	v, ok := kvs.Lookup(Comment)
	if ok {
		currKVS.Set(Comment, v)
	}

	hkvs := HelpSubSysMap[subSys]
	for _, hkv := range hkvs {
		var enabled bool
		if enableRequired {
			enabled = currKVS.Get(Enable) == EnableOn
		} else {
			// when enable arg is not required
			// then it is implicit on for the sub-system.
			enabled = true
		}
		v, _ := currKVS.Lookup(hkv.Key)
		if v == "" && !hkv.Optional && enabled {
			// Return error only if the
			// key is enabled, for state=off
			// let it be empty.
			return false, Errorf(
				"'%s' is not optional for '%s' sub-system, please check '%s' documentation",
				hkv.Key, subSys, subSys)
		}
	}
	c[subSys][tgt] = currKVS
	return dynamic, nil
}
