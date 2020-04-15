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
 */

package config

// HelpKV - implements help messages for keys
// with value as description of the keys.
type HelpKV struct {
	Key         string `json:"key"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Optional    bool   `json:"optional"`

	// Indicates if sub-sys supports multiple targets.
	MultipleTargets bool `json:"multipleTargets"`
}

// HelpKVS - implement order of keys help messages.
type HelpKVS []HelpKV

// Lookup - lookup a key from help kvs.
func (hkvs HelpKVS) Lookup(key string) (HelpKV, bool) {
	for _, hkv := range hkvs {
		if hkv.Key == key {
			return hkv, true
		}
	}
	return HelpKV{}, false
}

// DefaultComment used across all sub-systems.
const DefaultComment = "optionally add a comment to this setting"

// Region and Worm help is documented in default config
var (
	RegionHelp = HelpKVS{
		HelpKV{
			Key:         RegionName,
			Type:        "string",
			Description: `name of the location of the server e.g. "us-west-rack2"`,
			Optional:    true,
		},
		HelpKV{
			Key:         Comment,
			Type:        "sentence",
			Description: DefaultComment,
			Optional:    true,
		},
	}
)
