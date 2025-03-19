// Copyright (c) 2015-2023 MinIO, Inc.
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

package cmd

import (
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
)

// guessUserProvider - guesses the user provider based on the access key and claims.
func guessUserProvider(credentials auth.Credentials) string {
	if !credentials.IsServiceAccount() && !credentials.IsTemp() {
		return madmin.InternalProvider // regular users are always internal
	}

	claims := credentials.Claims
	if _, ok := claims[ldapUser]; ok {
		return madmin.LDAPProvider // ldap users
	}

	if _, ok := claims[subClaim]; ok {
		providerPrefix, _, found := strings.Cut(credentials.ParentUser, getKeySeparator())
		if found {
			return providerPrefix // this is true for certificate and custom providers
		}
		return madmin.OpenIDProvider // openid users are already hashed, so no separator
	}

	return madmin.InternalProvider // default to internal
}

// getProviderInfoFromClaims - returns the provider info from the claims.
func getProviderInfoFromClaims(claims map[string]interface{}, provider string) map[string]string {
	switch provider {
	case madmin.LDAPProvider:
		return getLDAPInfoFromClaims(claims)
	case madmin.OpenIDProvider:
		return getOpenIDInfoFromClaims(claims)
	}
	return nil
}

func getOpenIDCfgNameFromClaims(claims map[string]interface{}) (string, bool) {
	roleArn := claims[roleArnClaim]

	s := globalServerConfig.Clone()
	configs, err := globalIAMSys.OpenIDConfig.GetConfigList(s)
	if err != nil {
		return "", false
	}
	for _, cfg := range configs {
		if cfg.RoleARN == roleArn {
			return cfg.Name, true
		}
	}
	return "", false

}

func getOpenIDInfoFromClaims(claims map[string]interface{}) map[string]string {
	info := make(map[string]string)

	cfgName, ok := getOpenIDCfgNameFromClaims(claims)
	if !ok {
		return nil
	}

	info["Config Name"] = cfgName
	if displayNameClaim := globalIAMSys.OpenIDConfig.GetUserReadableClaim(cfgName); displayNameClaim != "" {
		name, _ := claims[displayNameClaim].(string)
		info["Display Name"] = name
		info["Display Name Claim"] = displayNameClaim
	}
	if idClaim := globalIAMSys.OpenIDConfig.GetUserIDClaim(cfgName); idClaim != "" {
		id, _ := claims[idClaim].(string)
		info["User ID"] = id
		info["User ID Claim"] = idClaim
	}

	return info
}

func getLDAPInfoFromClaims(claims map[string]interface{}) map[string]string {
	info := make(map[string]string)

	if name, ok := claims[ldapUser].(string); ok {
		info["User Name"] = name
	}

	return info
}
