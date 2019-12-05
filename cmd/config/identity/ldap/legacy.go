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

package ldap

import "github.com/minio/minio/cmd/config"

// SetIdentityLDAP - One time migration code needed, for migrating from older config to new for LDAPConfig.
func SetIdentityLDAP(s config.Config, ldapArgs Config) {
	if !ldapArgs.Enabled {
		// ldap not enabled no need to preserve it in new settings.
		return
	}
	s[config.IdentityLDAPSubSys][config.Default] = config.KVS{
		config.KV{
			Key:   ServerAddr,
			Value: ldapArgs.ServerAddr,
		},
		config.KV{
			Key:   STSExpiry,
			Value: ldapArgs.STSExpiryDuration,
		},
		config.KV{
			Key:   UsernameFormat,
			Value: ldapArgs.UsernameFormat,
		},
		config.KV{
			Key:   GroupSearchFilter,
			Value: ldapArgs.GroupSearchFilter,
		},
		config.KV{
			Key:   GroupNameAttribute,
			Value: ldapArgs.GroupNameAttribute,
		},
		config.KV{
			Key:   GroupSearchBaseDN,
			Value: ldapArgs.GroupSearchBaseDN,
		},
	}
}
