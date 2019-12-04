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

// Help template for LDAP identity feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ServerAddr,
			Description: `AD/LDAP server address e.g. "myldapserver.com:636"`,
			Type:        "address",
		},
		config.HelpKV{
			Key:         UsernameFormat,
			Description: `AD/LDAP format of full username DN e.g. "uid={username},cn=accounts,dc=myldapserver,dc=com"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         GroupSearchFilter,
			Description: `search filter to find groups of a user (optional) e.g. "(&(objectclass=groupOfNames)(member={usernamedn}))"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         GroupNameAttribute,
			Description: `attribute of search results to use as group name (optional) e.g. "cn"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         GroupSearchBaseDN,
			Description: `base DN in AD/LDAP hierarchy to use in search requests (optional) e.g. "dc=myldapserver,dc=com"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         STSExpiry,
			Description: `AD/LDAP STS credentials validity duration e.g. "1h"`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         TLSSkipVerify,
			Description: "enable this to disable client verification of server certificates",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
