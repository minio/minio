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

import (
	"github.com/minio/minio/internal/config"
)

// LegacyConfig contains AD/LDAP server connectivity information from old config
// V33.
type LegacyConfig struct {
	Enabled bool `json:"enabled"`

	// E.g. "ldap.minio.io:636"
	ServerAddr string `json:"serverAddr"`

	// User DN search parameters
	UserDNSearchBaseDistName  string   `json:"userDNSearchBaseDN"`
	UserDNSearchBaseDistNames []string `json:"-"` // Generated field
	UserDNSearchFilter        string   `json:"userDNSearchFilter"`

	// Group search parameters
	GroupSearchBaseDistName  string   `json:"groupSearchBaseDN"`
	GroupSearchBaseDistNames []string `json:"-"` // Generated field
	GroupSearchFilter        string   `json:"groupSearchFilter"`

	// Lookup bind LDAP service account
	LookupBindDN       string `json:"lookupBindDN"`
	LookupBindPassword string `json:"lookupBindPassword"`
}

// SetIdentityLDAP - One time migration code needed, for migrating from older config to new for LDAPConfig.
func SetIdentityLDAP(s config.Config, ldapArgs LegacyConfig) {
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
			Key:   GroupSearchFilter,
			Value: ldapArgs.GroupSearchFilter,
		},
		config.KV{
			Key:   GroupSearchBaseDN,
			Value: ldapArgs.GroupSearchBaseDistName,
		},
	}
}
