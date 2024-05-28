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

package ldap

import "github.com/minio/minio/internal/config"

// Help template for LDAP identity feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ServerAddr,
			Description: `AD/LDAP server address e.g. "myldap.com" or "myldapserver.com:636"` + defaultHelpPostfix(ServerAddr),
			Type:        "address",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         SRVRecordName,
			Description: `DNS SRV record name for LDAP service, if given, must be one of "ldap", "ldaps" or "on"` + defaultHelpPostfix(SRVRecordName),
			Optional:    true,
			Type:        "string",
			Sensitive:   false,
		},
		config.HelpKV{
			Key:         LookupBindDN,
			Description: `DN for LDAP read-only service account used to perform DN and group lookups` + defaultHelpPostfix(LookupBindDN),
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         LookupBindPassword,
			Description: `Password for LDAP read-only service account used to perform DN and group lookups` + defaultHelpPostfix(LookupBindPassword),
			Optional:    true,
			Type:        "string",
			Sensitive:   true,
			Secret:      true,
		},
		config.HelpKV{
			Key:         UserDNSearchBaseDN,
			Description: `";" separated list of user search base DNs e.g. "dc=myldapserver,dc=com"` + defaultHelpPostfix(UserDNSearchBaseDN),
			Optional:    true,
			Type:        "list",
		},
		config.HelpKV{
			Key:         UserDNSearchFilter,
			Description: `Search filter to lookup user DN` + defaultHelpPostfix(UserDNSearchFilter),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         UserDNAttributes,
			Description: `"," separated list of user DN attributes e.g. "uid,cn,mail,sshPublicKey"` + defaultHelpPostfix(UserDNAttributes),
			Optional:    true,
			Type:        "list",
		},
		config.HelpKV{
			Key:         GroupSearchFilter,
			Description: `search filter for groups e.g. "(&(objectclass=groupOfNames)(memberUid=%s))"` + defaultHelpPostfix(GroupSearchFilter),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         GroupSearchBaseDN,
			Description: `";" separated list of group search base DNs e.g. "dc=myldapserver,dc=com"` + defaultHelpPostfix(GroupSearchBaseDN),
			Optional:    true,
			Type:        "list",
		},
		config.HelpKV{
			Key:         TLSSkipVerify,
			Description: `trust server TLS without verification` + defaultHelpPostfix(TLSSkipVerify),
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         ServerInsecure,
			Description: `allow plain text connection to AD/LDAP server` + defaultHelpPostfix(ServerInsecure),
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         ServerStartTLS,
			Description: `use StartTLS connection to AD/LDAP server` + defaultHelpPostfix(ServerStartTLS),
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
