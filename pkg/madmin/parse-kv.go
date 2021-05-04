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

package madmin

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

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

// Lookup - lookup a key in a list of KVS
func (kvs KVS) Lookup(key string) (string, bool) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return "", false
}

// Target signifies an individual target
type Target struct {
	SubSystem string `json:"subSys"`
	KVS       KVS    `json:"kvs"`
}

// Standard config keys and values.
const (
	EnableKey  = "enable"
	CommentKey = "comment"

	// Enable values
	EnableOn  = "on"
	EnableOff = "off"
)

// HasSpace - returns if given string has space.
func HasSpace(s string) bool {
	for _, r := range s {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// Constant separators
const (
	SubSystemSeparator = `:`
	KvSeparator        = `=`
	KvComment          = `#`
	KvSpaceSeparator   = ` `
	KvNewline          = "\n"
	KvDoubleQuote      = `"`
	KvSingleQuote      = `'`

	Default = `_`
)

// SanitizeValue - this function is needed, to trim off single or double quotes, creeping into the values.
func SanitizeValue(v string) string {
	v = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(v), KvDoubleQuote), KvDoubleQuote)
	return strings.TrimSuffix(strings.TrimPrefix(v, KvSingleQuote), KvSingleQuote)
}

// KvFields - converts an input string of form "k1=v1 k2=v2" into
// fields of ["k1=v1", "k2=v2"], the tokenization of each `k=v`
// happens with the right number of input keys, if keys
// input is empty returned value is empty slice as well.
func KvFields(input string, keys []string) []string {
	var valueIndexes []int
	for _, key := range keys {
		i := strings.Index(input, key+KvSeparator)
		if i == -1 {
			continue
		}
		valueIndexes = append(valueIndexes, i)
	}

	sort.Ints(valueIndexes)
	var fields = make([]string, len(valueIndexes))
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

// ParseTarget - adds new targets, by parsing the input string s.
func ParseTarget(s string, help Help) (*Target, error) {
	inputs := strings.SplitN(s, KvSpaceSeparator, 2)
	if len(inputs) <= 1 {
		return nil, fmt.Errorf("invalid number of arguments '%s'", s)
	}

	subSystemValue := strings.SplitN(inputs[0], SubSystemSeparator, 2)
	if len(subSystemValue) == 0 {
		return nil, fmt.Errorf("invalid number of arguments %s", s)
	}

	if help.SubSys != subSystemValue[0] {
		return nil, fmt.Errorf("unknown sub-system %s", subSystemValue[0])
	}

	var kvs = KVS{}
	var prevK string
	for _, v := range KvFields(inputs[1], help.Keys()) {
		kv := strings.SplitN(v, KvSeparator, 2)
		if len(kv) == 0 {
			continue
		}
		if len(kv) == 1 && prevK != "" {
			value := strings.Join([]string{
				kvs.Get(prevK),
				SanitizeValue(kv[0]),
			}, KvSpaceSeparator)
			kvs.Set(prevK, value)
			continue
		}
		if len(kv) == 2 {
			prevK = kv[0]
			kvs.Set(prevK, SanitizeValue(kv[1]))
			continue
		}
		return nil, fmt.Errorf("value for key '%s' cannot be empty", kv[0])
	}

	return &Target{
		SubSystem: inputs[0],
		KVS:       kvs,
	}, nil
}

// ParseSubSysTarget - parse a sub-system target
func ParseSubSysTarget(buf []byte, help Help) (target *Target, err error) {
	bio := bufio.NewScanner(bytes.NewReader(buf))
	if bio.Scan() {
		return ParseTarget(bio.Text(), help)
	}
	return nil, bio.Err()
}
