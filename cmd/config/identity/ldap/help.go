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
			Description: `";" separated list of username bind DNs e.g. "uid=%s,cn=accounts,dc=myldapserver,dc=com"`,
			Type:        "list",
		},
		config.HelpKV{
			Key:         UsernameSearchFilter,
			Description: `user search filter, for example "(cn=%s)" or "(sAMAccountName=%s)" or "(uid=%s)"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         GroupSearchFilter,
			Description: `search filter for groups e.g. "(&(objectclass=groupOfNames)(memberUid=%s))"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         GroupSearchBaseDN,
			Description: `";" separated list of group search base DNs e.g. "dc=myldapserver,dc=com"`,
			Type:        "list",
		},
		config.HelpKV{
			Key:         UsernameSearchBaseDN,
			Description: `";" separated list of username search DNs`,
			Type:        "list",
			Optional:    true,
		},
		config.HelpKV{
			Key:         GroupNameAttribute,
			Description: `search attribute for group name e.g. "cn"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         STSExpiry,
			Description: `temporary credentials validity duration in s,m,h,d. Default is "1h"`,
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         TLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "off" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         ServerInsecure,
			Description: `allow plain text connection to AD/LDAP server, defaults to "off"`,
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
