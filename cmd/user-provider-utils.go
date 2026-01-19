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
	"context"
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
)

// getUserWithProvider - returns the appropriate internal username based on the user provider.
// if validate is true, an error is returned if the user does not exist.
func getUserWithProvider(ctx context.Context, userProvider, user string, validate bool) (string, error) {
	switch userProvider {
	case madmin.BuiltinProvider:
		if validate {
			if _, ok := globalIAMSys.GetUser(ctx, user); !ok {
				return "", errNoSuchUser
			}
		}
		return user, nil
	case madmin.LDAPProvider:
		if globalIAMSys.GetUsersSysType() != LDAPUsersSysType {
			return "", errIAMActionNotAllowed
		}
		res, err := globalIAMSys.LDAPConfig.GetValidatedDNForUsername(user)
		if res == nil {
			err = errNoSuchUser
		}
		if err != nil {
			if validate {
				return "", err
			}
			if !globalIAMSys.LDAPConfig.ParsesAsDN(user) {
				return "", errNoSuchUser
			}
		}
		return res.NormDN, nil
	default:
		return "", errIAMActionNotAllowed
	}
}

// guessUserProvider - guesses the user provider based on the access key and claims.
func guessUserProvider(credentials auth.Credentials) string {
	if !credentials.IsServiceAccount() && !credentials.IsTemp() {
		return madmin.BuiltinProvider // regular users are always internal
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

	return madmin.BuiltinProvider // default to internal
}

// getProviderInfoFromClaims - returns the provider info from the claims.
func populateProviderInfoFromClaims(claims map[string]any, provider string, resp *madmin.InfoAccessKeyResp) {
	resp.UserProvider = provider
	switch provider {
	case madmin.LDAPProvider:
		resp.LDAPSpecificInfo = getLDAPInfoFromClaims(claims)
	case madmin.OpenIDProvider:
		resp.OpenIDSpecificInfo = getOpenIDInfoFromClaims(claims)
	}
}

func getOpenIDCfgNameFromClaims(claims map[string]any) (string, bool) {
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

func getOpenIDInfoFromClaims(claims map[string]any) madmin.OpenIDSpecificAccessKeyInfo {
	info := madmin.OpenIDSpecificAccessKeyInfo{}

	cfgName, ok := getOpenIDCfgNameFromClaims(claims)
	if !ok {
		return info
	}

	info.ConfigName = cfgName
	if displayNameClaim := globalIAMSys.OpenIDConfig.GetUserReadableClaim(cfgName); displayNameClaim != "" {
		name, _ := claims[displayNameClaim].(string)
		info.DisplayName = name
		info.DisplayNameClaim = displayNameClaim
	}
	if idClaim := globalIAMSys.OpenIDConfig.GetUserIDClaim(cfgName); idClaim != "" {
		id, _ := claims[idClaim].(string)
		info.UserID = id
		info.UserIDClaim = idClaim
	}

	return info
}

func getLDAPInfoFromClaims(claims map[string]any) madmin.LDAPSpecificAccessKeyInfo {
	info := madmin.LDAPSpecificAccessKeyInfo{}

	if name, ok := claims[ldapUser].(string); ok {
		info.Username = name
	}

	return info
}
