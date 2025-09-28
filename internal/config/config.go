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
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/v3/env"
)

// ErrorConfig holds the config error types
type ErrorConfig interface {
	ErrConfigGeneric | ErrConfigNotFound
}

// ErrConfigGeneric is a generic config type
type ErrConfigGeneric struct {
	msg string
}

func (ge *ErrConfigGeneric) setMsg(msg string) {
	ge.msg = msg
}

func (ge ErrConfigGeneric) Error() string {
	return ge.msg
}

// ErrConfigNotFound is an error to indicate
// that a config parameter is not found
type ErrConfigNotFound struct {
	ErrConfigGeneric
}

// Error creates an error message and wraps
// it with the error type specified in the type parameter
func Error[T ErrorConfig, PT interface {
	*T
	setMsg(string)
}](format string, vals ...any,
) T {
	pt := PT(new(T))
	pt.setMsg(fmt.Sprintf(format, vals...))
	return *pt
}

// Errorf formats an error and returns it as a generic config error
func Errorf(format string, vals ...any) ErrConfigGeneric {
	return Error[ErrConfigGeneric](format, vals...)
}

// Default keys
const (
	Default = madmin.Default
	Enable  = madmin.EnableKey
	Comment = madmin.CommentKey

	EnvSeparator = "="

	// Enable values
	EnableOn  = madmin.EnableOn
	EnableOff = madmin.EnableOff

	RegionKey  = "region"
	NameKey    = "name"
	RegionName = "name"
	AccessKey  = "access_key"
	SecretKey  = "secret_key"
	License    = "license" // Deprecated Dec 2021
	APIKey     = "api_key"
	Proxy      = "proxy"
)

// Top level config constants.
const (
	PolicyOPASubSys      = madmin.PolicyOPASubSys
	PolicyPluginSubSys   = madmin.PolicyPluginSubSys
	IdentityOpenIDSubSys = madmin.IdentityOpenIDSubSys
	IdentityLDAPSubSys   = madmin.IdentityLDAPSubSys
	IdentityTLSSubSys    = madmin.IdentityTLSSubSys
	IdentityPluginSubSys = madmin.IdentityPluginSubSys
	SiteSubSys           = madmin.SiteSubSys
	RegionSubSys         = madmin.RegionSubSys
	EtcdSubSys           = madmin.EtcdSubSys
	StorageClassSubSys   = madmin.StorageClassSubSys
	APISubSys            = madmin.APISubSys
	CompressionSubSys    = madmin.CompressionSubSys
	LoggerWebhookSubSys  = madmin.LoggerWebhookSubSys
	AuditWebhookSubSys   = madmin.AuditWebhookSubSys
	AuditKafkaSubSys     = madmin.AuditKafkaSubSys
	HealSubSys           = madmin.HealSubSys
	ScannerSubSys        = madmin.ScannerSubSys
	CrawlerSubSys        = madmin.CrawlerSubSys
	SubnetSubSys         = madmin.SubnetSubSys
	CallhomeSubSys       = madmin.CallhomeSubSys
	DriveSubSys          = madmin.DriveSubSys
	BatchSubSys          = madmin.BatchSubSys
	BrowserSubSys        = madmin.BrowserSubSys
	ILMSubSys            = madmin.ILMSubsys

	// Add new constants here (similar to above) if you add new fields to config.
)

// Notification config constants.
const (
	NotifyKafkaSubSys    = madmin.NotifyKafkaSubSys
	NotifyMQTTSubSys     = madmin.NotifyMQTTSubSys
	NotifyMySQLSubSys    = madmin.NotifyMySQLSubSys
	NotifyNATSSubSys     = madmin.NotifyNATSSubSys
	NotifyNSQSubSys      = madmin.NotifyNSQSubSys
	NotifyESSubSys       = madmin.NotifyESSubSys
	NotifyAMQPSubSys     = madmin.NotifyAMQPSubSys
	NotifyPostgresSubSys = madmin.NotifyPostgresSubSys
	NotifyRedisSubSys    = madmin.NotifyRedisSubSys
	NotifyWebhookSubSys  = madmin.NotifyWebhookSubSys

	// Add new constants here (similar to above) if you add new fields to config.
)

// Lambda config constants.
const (
	LambdaWebhookSubSys = madmin.LambdaWebhookSubSys
)

// NotifySubSystems - all notification sub-systems
var NotifySubSystems = set.CreateStringSet(
	NotifyKafkaSubSys,
	NotifyMQTTSubSys,
	NotifyMySQLSubSys,
	NotifyNATSSubSys,
	NotifyNSQSubSys,
	NotifyESSubSys,
	NotifyAMQPSubSys,
	NotifyPostgresSubSys,
	NotifyRedisSubSys,
	NotifyWebhookSubSys,
)

// LambdaSubSystems - all lambda sub-systems
var LambdaSubSystems = set.CreateStringSet(
	LambdaWebhookSubSys,
)

// LoggerSubSystems - all sub-systems related to logger
var LoggerSubSystems = set.CreateStringSet(
	LoggerWebhookSubSys,
	AuditWebhookSubSys,
	AuditKafkaSubSys,
)

// SubSystems - all supported sub-systems
var SubSystems = madmin.SubSystems

// SubSystemsDynamic - all sub-systems that have dynamic config.
var SubSystemsDynamic = set.CreateStringSet(
	APISubSys,
	CompressionSubSys,
	ScannerSubSys,
	HealSubSys,
	SubnetSubSys,
	CallhomeSubSys,
	DriveSubSys,
	LoggerWebhookSubSys,
	AuditWebhookSubSys,
	AuditKafkaSubSys,
	StorageClassSubSys,
	ILMSubSys,
	BatchSubSys,
	BrowserSubSys,
)

// SubSystemsSingleTargets - subsystems which only support single target.
var SubSystemsSingleTargets = set.CreateStringSet(
	SiteSubSys,
	RegionSubSys,
	EtcdSubSys,
	APISubSys,
	StorageClassSubSys,
	CompressionSubSys,
	PolicyOPASubSys,
	PolicyPluginSubSys,
	IdentityLDAPSubSys,
	IdentityTLSSubSys,
	IdentityPluginSubSys,
	HealSubSys,
	ScannerSubSys,
	SubnetSubSys,
	CallhomeSubSys,
	DriveSubSys,
	ILMSubSys,
	BatchSubSys,
	BrowserSubSys,
)

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
	EnvPrefix        = madmin.EnvPrefix
	EnvWordDelimiter = madmin.EnvWordDelimiter
)

// DefaultKVS - default kvs for all sub-systems
var DefaultKVS = map[string]KVS{}

// RegisterDefaultKVS - this function saves input kvsMap
// globally, this should be called only once preferably
// during `init()`.
func RegisterDefaultKVS(kvsMap map[string]KVS) {
	maps.Copy(DefaultKVS, kvsMap)
}

// HelpSubSysMap - help for all individual KVS for each sub-systems
// also carries a special empty sub-system which dumps
// help for each sub-system key.
var HelpSubSysMap = map[string]HelpKVS{}

// RegisterHelpSubSys - this function saves
// input help KVS for each sub-system globally,
// this function should be called only once
// preferably in during `init()`.
func RegisterHelpSubSys(helpKVSMap map[string]HelpKVS) {
	maps.Copy(HelpSubSysMap, helpKVSMap)
}

// HelpDeprecatedSubSysMap - help for all deprecated sub-systems, that may be
// removed in the future.
var HelpDeprecatedSubSysMap = map[string]HelpKV{}

// RegisterHelpDeprecatedSubSys - saves input help KVS for deprecated
// sub-systems globally. Should be called only once at init.
func RegisterHelpDeprecatedSubSys(helpDeprecatedKVMap map[string]HelpKV) {
	maps.Copy(HelpDeprecatedSubSysMap, helpDeprecatedKVMap)
}

// KV - is a shorthand of each key value.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`

	HiddenIfEmpty bool `json:"-"`
}

func (kv KV) String() string {
	var s strings.Builder
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
	return s.String()
}

// KVS - is a shorthand for some wrapper functions
// to operate on list of key values.
type KVS []KV

// Empty - return if kv is empty
func (kvs KVS) Empty() bool {
	return len(kvs) == 0
}

// Clone - returns a copy of the KVS
func (kvs KVS) Clone() KVS {
	return append(make(KVS, 0, len(kvs)), kvs...)
}

// GetWithDefault - returns default value if key not set
func (kvs KVS) GetWithDefault(key string, defaultKVS KVS) string {
	v := kvs.Get(key)
	if len(v) == 0 {
		return defaultKVS.Get(key)
	}
	return v
}

// Keys returns the list of keys for the current KVS
func (kvs KVS) Keys() []string {
	keys := make([]string, len(kvs))
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
		s.WriteString(kv.String())
		s.WriteString(KvSpaceSeparator)
	}
	return s.String()
}

// Merge environment values with on disk KVS, environment values overrides
// anything on the disk.
func Merge(cfgKVS map[string]KVS, envname string, defaultKVS KVS) map[string]KVS {
	newCfgKVS := make(map[string]KVS)
	for _, e := range env.List(envname) {
		tgt := strings.TrimPrefix(e, envname+Default)
		if tgt == envname {
			tgt = Default
		}
		newCfgKVS[tgt] = defaultKVS
	}
	maps.Copy(newCfgKVS, cfgKVS)
	return newCfgKVS
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

// LookupKV returns the KV by its key
func (kvs KVS) LookupKV(key string) (KV, bool) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv, true
		}
	}
	return KV{}, false
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

// ContextKeyString is type(string) for contextKey
type ContextKeyString string

// ContextKeyForTargetFromConfig - key for context for target from config
const ContextKeyForTargetFromConfig = ContextKeyString("ContextKeyForTargetFromConfig")

// ParseConfigTargetID - read all targetIDs from reader
func ParseConfigTargetID(r io.Reader) (ids map[string]bool, err error) {
	ids = make(map[string]bool)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// Skip any empty lines, or comment like characters
		text := scanner.Text()
		if text == "" || strings.HasPrefix(text, KvComment) {
			continue
		}
		_, _, tgt, err := GetSubSys(text)
		if err != nil {
			return nil, err
		}
		ids[tgt] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ids, err
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
	}

	return nc
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

	DefaultSiteKVS = KVS{
		KV{
			Key:   NameKey,
			Value: "",
		},
		KV{
			Key:   RegionKey,
			Value: "",
		},
	}

	DefaultRegionKVS = KVS{
		KV{
			Key:   RegionName,
			Value: "",
		},
	}
)

var siteLK sync.RWMutex

// Site - holds site info - name and region.
type Site struct {
	name   string
	region string
}

// Update safe update the new site name and region
func (s *Site) Update(n Site) {
	siteLK.Lock()
	s.name = n.name
	s.region = n.region
	siteLK.Unlock()
}

// Name returns currently configured site name
func (s *Site) Name() string {
	siteLK.RLock()
	defer siteLK.RUnlock()

	return s.name
}

// Region returns currently configured site region
func (s *Site) Region() string {
	siteLK.RLock()
	defer siteLK.RUnlock()

	return s.region
}

var validRegionRegex = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9-_-]+$")

// validSiteNameRegex - allows lowercase letters, digits and '-', starts with
// letter. At least 2 characters long.
var validSiteNameRegex = regexp.MustCompile("^[a-z][a-z0-9-]+$")

// LookupSite - get site related configuration. Loads configuration from legacy
// region sub-system as well.
func LookupSite(siteKV KVS, regionKV KVS) (s Site, err error) {
	if err = CheckValidKeys(SiteSubSys, siteKV, DefaultSiteKVS); err != nil {
		return s, err
	}
	region := env.Get(EnvRegion, "")
	if region == "" {
		env.Get(EnvRegionName, "")
	}
	if region == "" {
		region = env.Get(EnvSiteRegion, siteKV.Get(RegionKey))
	}
	if region == "" {
		// No region config found in the site-subsystem. So lookup the legacy
		// region sub-system.
		if err = CheckValidKeys(RegionSubSys, regionKV, DefaultRegionKVS); err != nil {
			// An invalid key was found in the region sub-system.
			// Since the region sub-system cannot be (re)set as it
			// is legacy, we return an error to tell the user to
			// reset the region via the new command.
			err = Errorf("could not load region from legacy configuration as it was invalid - use 'mc admin config set myminio site region=myregion name=myname' to set a region and name (%v)", err)
			return s, err
		}

		region = regionKV.Get(RegionName)
	}
	if region != "" {
		if !validRegionRegex.MatchString(region) {
			err = Errorf(
				"region '%s' is invalid, expected simple characters such as [us-east-1, myregion...]",
				region)
			return s, err
		}
		s.region = region
	}

	name := env.Get(EnvSiteName, siteKV.Get(NameKey))
	if name != "" {
		if !validSiteNameRegex.MatchString(name) {
			err = Errorf(
				"site name '%s' is invalid, expected simple characters such as [cal-rack0, myname...]",
				name)
			return s, err
		}
		s.name = name
	}
	return s, err
}

// CheckValidKeys - checks if inputs KVS has the necessary keys,
// returns error if it find extra or superfluous keys.
func CheckValidKeys(subSys string, kv KVS, validKVS KVS, deprecatedKeys ...string) error {
	nkv := KVS{}
	for _, kv := range kv {
		// Comment is a valid key, its also fully optional
		// ignore it since it is a valid key for all
		// sub-systems.
		if kv.Key == Comment {
			continue
		}
		var skip bool
		if slices.Contains(deprecatedKeys, kv.Key) {
			skip = true
		}
		if skip {
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

const ( // deprecated keys
	apiReplicationWorkers       = "replication_workers"
	apiReplicationFailedWorkers = "replication_failed_workers"
)

// map of subsystem to deleted keys
var deletedSubSysKeys = map[string][]string{
	APISubSys: {apiReplicationWorkers, apiReplicationFailedWorkers},
	// Add future sub-system deleted keys
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
			// Delete deprecated keys for subsystem if any
			if keys, ok := deletedSubSysKeys[subSys]; ok {
				for _, key := range keys {
					ckvs.Delete(key)
				}
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
		// Use help for sub-system to preserve the order. Add deprecated
		// keys at the end (in some order).
		kvsOrder := append([]HelpKV{}, HelpSubSysMap[""]...)
		for _, v := range HelpDeprecatedSubSysMap {
			kvsOrder = append(kvsOrder, v)
		}

		for _, hkv := range kvsOrder {
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
	subSys, inputs, tgt, err := GetSubSys(s)
	if err != nil {
		if !SubSystems.Contains(subSys) && len(inputs) == 1 {
			// Unknown sub-system found try to remove it anyways.
			delete(c, subSys)
			return nil
		}
		return err
	}

	ck, ok := c[subSys][tgt]
	if !ok {
		return Error[ErrConfigNotFound]("sub-system %s:%s already deleted or does not exist", subSys, tgt)
	}

	if len(inputs) == 2 {
		currKVS := ck.Clone()
		defKVS := DefaultKVS[subSys]
		for delKey := range strings.FieldsSeq(inputs[1]) {
			_, ok := currKVS.Lookup(delKey)
			if !ok {
				return Error[ErrConfigNotFound]("key %s doesn't exist", delKey)
			}
			defVal, isDef := defKVS.Lookup(delKey)
			if isDef {
				currKVS.Set(delKey, defVal)
			} else {
				currKVS.Delete(delKey)
			}
		}
		c[subSys][tgt] = currKVS
	} else {
		delete(c[subSys], tgt)
	}
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

// GetSubSys - extracts subssystem info from given config string
func GetSubSys(s string) (subSys string, inputs []string, tgt string, e error) {
	tgt = Default
	if len(s) == 0 {
		return subSys, inputs, tgt, Errorf("input arguments cannot be empty")
	}
	inputs = strings.SplitN(s, KvSpaceSeparator, 2)

	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	subSys = subSystemValue[0]
	if !SubSystems.Contains(subSys) {
		return subSys, inputs, tgt, Errorf("unknown sub-system %s", s)
	}

	if SubSystemsSingleTargets.Contains(subSystemValue[0]) && len(subSystemValue) == 2 {
		return subSys, inputs, tgt, Errorf("sub-system '%s' only supports single target", subSystemValue[0])
	}

	if len(subSystemValue) == 2 {
		tgt = subSystemValue[1]
	}

	return subSys, inputs, tgt, e
}

// kvFields - converts an input string of form "k1=v1 k2=v2" into
// fields of ["k1=v1", "k2=v2"], the tokenization of each `k=v`
// happens with the right number of input keys, if keys
// input is empty returned value is empty slice as well.
func kvFields(input string, keys []string) []string {
	valueIndexes := make([]int, 0, len(keys))
	for _, key := range keys {
		i := strings.Index(input, key+KvSeparator)
		if i == -1 {
			continue
		}
		valueIndexes = append(valueIndexes, i)
	}

	sort.Ints(valueIndexes)
	fields := make([]string, len(valueIndexes))
	for i := range valueIndexes {
		j := i + 1
		if j < len(valueIndexes) {
			fields[i] = strings.TrimSpace(input[valueIndexes[i]:valueIndexes[j]])
		} else {
			fields[i] = strings.TrimSpace(input[valueIndexes[i]:])
		}
	}
	return fields
}

// SetKVS - set specific key values per sub-system.
func (c Config) SetKVS(s string, defaultKVS map[string]KVS) (dynamic bool, err error) {
	subSys, inputs, tgt, err := GetSubSys(s)
	if err != nil {
		return false, err
	}
	if len(inputs) < 2 {
		return false, Errorf("sub-system '%s' must have key", subSys)
	}

	dynamic = SubSystemsDynamic.Contains(subSys)

	fields := kvFields(inputs[1], defaultKVS[subSys].Keys())
	if len(fields) == 0 {
		return false, Errorf("sub-system '%s' cannot have empty keys", subSys)
	}

	kvs := KVS{}
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

	var currKVS KVS
	ck, ok := c[subSys][tgt]
	if !ok {
		currKVS = defaultKVS[subSys].Clone()
	} else {
		currKVS = ck.Clone()
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

// CheckValidKeys - checks if the config parameters for the given subsystem and
// target are valid. It checks both the configuration store as well as
// environment variables.
func (c Config) CheckValidKeys(subSys string, deprecatedKeys []string) error {
	defKVS, ok := DefaultKVS[subSys]
	if !ok {
		return Errorf("Subsystem %s does not exist", subSys)
	}

	// Make a list of valid keys for the subsystem including the `comment`
	// key.
	validKeys := make([]string, 0, len(defKVS)+1)
	for _, param := range defKVS {
		validKeys = append(validKeys, param.Key)
	}
	validKeys = append(validKeys, Comment)

	subSysEnvVars := env.List(fmt.Sprintf("%s%s", EnvPrefix, strings.ToUpper(subSys)))

	// Set of env vars for the sub-system to validate.
	candidates := set.CreateStringSet(subSysEnvVars...)

	// Remove all default target env vars from the candidates set (as they
	// are valid).
	for _, param := range validKeys {
		paramEnvName := getEnvVarName(subSys, Default, param)
		candidates.Remove(paramEnvName)
	}

	isSingleTarget := SubSystemsSingleTargets.Contains(subSys)
	if isSingleTarget && len(candidates) > 0 {
		return Errorf("The following environment variables are unknown: %s",
			strings.Join(candidates.ToSlice(), ", "))
	}

	if !isSingleTarget {
		// Validate other env vars for all targets.
		envVars := candidates.ToSlice()
		for _, envVar := range envVars {
			for _, param := range validKeys {
				pEnvName := getEnvVarName(subSys, Default, param) + Default
				if len(envVar) > len(pEnvName) && strings.HasPrefix(envVar, pEnvName) {
					// This envVar is valid - it has a
					// non-empty target.
					candidates.Remove(envVar)
				}
			}
		}

		// Whatever remains are invalid env vars - return an error.
		if len(candidates) > 0 {
			return Errorf("The following environment variables are unknown: %s",
				strings.Join(candidates.ToSlice(), ", "))
		}
	}

	validKeysSet := set.CreateStringSet(validKeys...)
	validKeysSet = validKeysSet.Difference(set.CreateStringSet(deprecatedKeys...))
	kvsMap := c[subSys]
	for tgt, kvs := range kvsMap {
		invalidKV := KVS{}
		for _, kv := range kvs {
			if !validKeysSet.Contains(kv.Key) {
				invalidKV = append(invalidKV, kv)
			}
		}
		if len(invalidKV) > 0 {
			return Errorf(
				"found invalid keys (%s) for '%s:%s' sub-system, use 'mc admin config reset myminio %s:%s' to fix invalid keys",
				invalidKV.String(), subSys, tgt, subSys, tgt)
		}
	}
	return nil
}

// GetAvailableTargets - returns a list of targets configured for the given
// subsystem (whether they are enabled or not). A target could be configured via
// environment variables or via the configuration store. The default target is
// `_` and is always returned. The result is sorted so that the default target
// is the first one and the remaining entries are sorted in ascending order.
func (c Config) GetAvailableTargets(subSys string) ([]string, error) {
	if SubSystemsSingleTargets.Contains(subSys) {
		return []string{Default}, nil
	}

	defKVS, ok := DefaultKVS[subSys]
	if !ok {
		return nil, Errorf("Subsystem %s does not exist", subSys)
	}

	kvsMap := c[subSys]
	seen := set.NewStringSet()

	// Add all targets that are configured in the config store.
	for k := range kvsMap {
		seen.Add(k)
	}

	// env:prefix
	filterMap := map[string]string{}
	// Add targets that are configured via environment variables.
	for _, param := range defKVS {
		envVarPrefix := getEnvVarName(subSys, Default, param.Key) + Default
		envsWithPrefix := env.List(envVarPrefix)
		for _, k := range envsWithPrefix {
			tgtName := strings.TrimPrefix(k, envVarPrefix)
			if tgtName != "" {
				if v, ok := filterMap[k]; ok {
					if strings.HasPrefix(envVarPrefix, v) {
						filterMap[k] = envVarPrefix
					}
				} else {
					filterMap[k] = envVarPrefix
				}
			}
		}
	}

	for k, v := range filterMap {
		seen.Add(strings.TrimPrefix(k, v))
	}

	seen.Remove(Default)
	targets := seen.ToSlice()
	sort.Strings(targets)
	targets = append([]string{Default}, targets...)

	return targets, nil
}

func getEnvVarName(subSys, target, param string) string {
	if target == Default {
		return fmt.Sprintf("%s%s%s%s", EnvPrefix, strings.ToUpper(subSys), Default, strings.ToUpper(param))
	}

	return fmt.Sprintf("%s%s%s%s%s%s", EnvPrefix, strings.ToUpper(subSys), Default, strings.ToUpper(param),
		Default, target)
}

var resolvableSubsystems = set.CreateStringSet(IdentityOpenIDSubSys, IdentityLDAPSubSys, PolicyPluginSubSys)

// ValueSource represents the source of a config parameter value.
type ValueSource uint8

// Constants for ValueSource
const (
	ValueSourceAbsent ValueSource = iota // this is an error case
	ValueSourceDef
	ValueSourceCfg
	ValueSourceEnv
)

// ResolveConfigParam returns the effective value of a configuration parameter,
// within a subsystem and subsystem target. The effective value is, in order of
// decreasing precedence:
//
// 1. the value of the corresponding environment variable if set,
// 2. the value of the parameter in the config store if set,
// 3. the default value,
//
// This function only works for a subset of sub-systems, others return
// `ValueSourceAbsent`. FIXME: some parameters have custom environment
// variables for which support needs to be added.
//
// When redactSecrets is true, the returned value is empty if the configuration
// parameter is a secret, and the returned isRedacted flag is set.
func (c Config) ResolveConfigParam(subSys, target, cfgParam string, redactSecrets bool,
) (value string, cs ValueSource, isRedacted bool) {
	// cs = ValueSourceAbsent initially as it is iota by default.

	// Initially only support OpenID
	if !resolvableSubsystems.Contains(subSys) {
		return value, cs, isRedacted
	}

	// Check if config param requested is valid.
	defKVS, ok := DefaultKVS[subSys]
	if !ok {
		return value, cs, isRedacted
	}

	defValue, isFound := defKVS.Lookup(cfgParam)
	// Comments usually are absent from `defKVS`, so we handle it specially.
	if !isFound && cfgParam == Comment {
		defValue, isFound = "", true
	}
	if !isFound {
		return value, cs, isRedacted
	}

	if target == "" {
		target = Default
	}

	if redactSecrets {
		// If the configuration parameter is a secret, make sure to redact it when
		// we return.
		helpKV, _ := HelpSubSysMap[subSys].Lookup(cfgParam)
		if helpKV.Secret {
			defer func() {
				value = ""
				isRedacted = true
			}()
		}
	}

	envVar := getEnvVarName(subSys, target, cfgParam)

	// Lookup Env var.
	value = env.Get(envVar, "")
	if value != "" {
		cs = ValueSourceEnv
		return value, cs, isRedacted
	}

	// Lookup config store.
	if subSysStore, ok := c[subSys]; ok {
		if kvs, ok2 := subSysStore[target]; ok2 {
			var ok3 bool
			value, ok3 = kvs.Lookup(cfgParam)
			if ok3 {
				cs = ValueSourceCfg
				return value, cs, isRedacted
			}
		}
	}

	// Return the default value.
	value = defValue
	cs = ValueSourceDef
	return value, cs, isRedacted
}

// KVSrc represents a configuration parameter key and value along with the
// source of the value.
type KVSrc struct {
	Key   string
	Value string
	Src   ValueSource
}

// GetResolvedConfigParams returns all applicable config parameters with their
// value sources.
func (c Config) GetResolvedConfigParams(subSys, target string, redactSecrets bool) ([]KVSrc, error) {
	if !resolvableSubsystems.Contains(subSys) {
		return nil, Errorf("unsupported subsystem: %s", subSys)
	}

	// Check if config param requested is valid.
	defKVS, ok := DefaultKVS[subSys]
	if !ok {
		return nil, Errorf("unknown subsystem: %s", subSys)
	}

	r := make([]KVSrc, 0, len(defKVS)+1)
	for _, kv := range defKVS {
		v, vs, isRedacted := c.ResolveConfigParam(subSys, target, kv.Key, redactSecrets)

		// Fix `vs` when default.
		if v == kv.Value {
			vs = ValueSourceDef
		}

		if redactSecrets && isRedacted {
			// Skip adding redacted secrets to the output.
			continue
		}

		r = append(r, KVSrc{
			Key:   kv.Key,
			Value: v,
			Src:   vs,
		})
	}

	// Add the comment key as well if non-empty (and comments are never
	// redacted).
	v, vs, _ := c.ResolveConfigParam(subSys, target, Comment, redactSecrets)
	if vs != ValueSourceDef {
		r = append(r, KVSrc{
			Key:   Comment,
			Value: v,
			Src:   vs,
		})
	}

	return r, nil
}

// getTargetKVS returns configuration KVs for the given subsystem and target. It
// does not return any secrets in the configuration values when `redactSecrets`
// is set.
func (c Config) getTargetKVS(subSys, target string, redactSecrets bool) KVS {
	store, ok := c[subSys]
	if !ok {
		return nil
	}

	// Lookup will succeed, because this function only works with valid subSys
	// values.
	resultKVS := make([]KV, 0, len(store[target]))
	hkvs := HelpSubSysMap[subSys]
	for _, kv := range store[target] {
		hkv, _ := hkvs.Lookup(kv.Key)
		if hkv.Secret && redactSecrets && kv.Value != "" {
			// Skip returning secrets.
			continue
			// clonedKV := kv
			// clonedKV.Value = redactedSecret
			// resultKVS = append(resultKVS, clonedKV)
		}
		resultKVS = append(resultKVS, kv)
	}

	return resultKVS
}

// getTargetEnvs returns configured environment variable settings for the given
// subsystem and target.
func (c Config) getTargetEnvs(subSys, target string, defKVS KVS, redactSecrets bool) map[string]EnvPair {
	hkvs := HelpSubSysMap[subSys]
	envMap := make(map[string]EnvPair)

	// Add all env vars that are set.
	for _, kv := range defKVS {
		envName := getEnvVarName(subSys, target, kv.Key)
		envPair := EnvPair{
			Name:  envName,
			Value: env.Get(envName, ""),
		}
		if envPair.Value != "" {
			hkv, _ := hkvs.Lookup(kv.Key)
			if hkv.Secret && redactSecrets {
				// Skip adding any secret to the returned value.
				continue
				// envPair.Value = redactedSecret
			}
			envMap[kv.Key] = envPair
		}
	}
	return envMap
}

// EnvPair represents an environment variable and its value.
type EnvPair struct {
	Name, Value string
}

// SubsysInfo holds config info for a subsystem target.
type SubsysInfo struct {
	SubSys, Target string
	Defaults       KVS
	Config         KVS

	// map of config parameter name to EnvPair.
	EnvMap map[string]EnvPair
}

// GetSubsysInfo returns `SubsysInfo`s for all targets for the subsystem, when
// target is empty. Otherwise returns `SubsysInfo` for the desired target only.
// To request the default target only, target must be set to `Default`.
func (c Config) GetSubsysInfo(subSys, target string, redactSecrets bool) ([]SubsysInfo, error) {
	// Check if config param requested is valid.
	defKVS1, ok := DefaultKVS[subSys]
	if !ok {
		return nil, Errorf("unknown subsystem: %s", subSys)
	}

	targets, err := c.GetAvailableTargets(subSys)
	if err != nil {
		return nil, err
	}

	if target != "" {
		found := slices.Contains(targets, target)
		if !found {
			return nil, Errorf("there is no target `%s` for subsystem `%s`", target, subSys)
		}
		targets = []string{target}
	}

	// The `Comment` configuration variable is optional but is available to be
	// set for all sub-systems. It is not present in the `DefaultKVS` map's
	// values. To enable fetching a configured comment value from the
	// environment we add it to the list of default keys for the subsystem.
	defKVS := make([]KV, len(defKVS1), len(defKVS1)+1)
	copy(defKVS, defKVS1)
	defKVS = append(defKVS, KV{Key: Comment})

	r := make([]SubsysInfo, 0, len(targets))
	for _, target := range targets {
		r = append(r, SubsysInfo{
			SubSys:   subSys,
			Target:   target,
			Defaults: defKVS,
			Config:   c.getTargetKVS(subSys, target, redactSecrets),
			EnvMap:   c.getTargetEnvs(subSys, target, defKVS, redactSecrets),
		})
	}

	return r, nil
}

// AddEnvString adds env vars to the given string builder.
func (cs *SubsysInfo) AddEnvString(b *strings.Builder) {
	for _, v := range cs.Defaults {
		if ep, ok := cs.EnvMap[v.Key]; ok {
			b.WriteString(KvComment)
			b.WriteString(KvSpaceSeparator)
			b.WriteString(ep.Name)
			b.WriteString(EnvSeparator)
			b.WriteString(ep.Value)
			b.WriteString(KvNewline)
		}
	}
}

// WriteTo writes the string representation of the configuration to the given
// builder. When off is true, adds a comment character before the config system
// output. It also ignores values when empty and deprecated.
func (cs *SubsysInfo) WriteTo(b *strings.Builder, off bool) {
	cs.AddEnvString(b)
	if off {
		b.WriteString(KvComment)
		b.WriteString(KvSpaceSeparator)
	}
	b.WriteString(cs.SubSys)
	if cs.Target != Default {
		b.WriteString(SubSystemSeparator)
		b.WriteString(cs.Target)
	}
	b.WriteString(KvSpaceSeparator)
	for _, kv := range cs.Config {
		dkv, ok := cs.Defaults.LookupKV(kv.Key)
		if !ok {
			continue
		}
		// Ignore empty and deprecated values
		if dkv.HiddenIfEmpty && kv.Value == "" {
			continue
		}
		// Do not need to print if state is on
		if kv.Key == Enable && kv.Value == EnableOn {
			continue
		}
		b.WriteString(kv.String())
		b.WriteString(KvSpaceSeparator)
	}

	b.WriteString(KvNewline)
}
