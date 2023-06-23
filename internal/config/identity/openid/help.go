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

package openid

import "github.com/minio/minio/internal/config"

// Help template for OpenID identity feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         DisplayName,
			Description: "Friendly display name for this Provider/App" + defaultHelpPostfix(DisplayName),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ConfigURL,
			Description: `openid discovery document e.g. "https://accounts.google.com/.well-known/openid-configuration"` + defaultHelpPostfix(ConfigURL),
			Type:        "url",
		},
		config.HelpKV{
			Key:         ClientID,
			Description: `unique public identifier for apps e.g. "292085223830.apps.googleusercontent.com"` + defaultHelpPostfix(ClientID),
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClientSecret,
			Description: `secret for the unique public identifier for apps` + defaultHelpPostfix(ClientSecret),
			Sensitive:   true,
			Type:        "string",
			Secret:      true,
		},
		config.HelpKV{
			Key:         RolePolicy,
			Description: `Set the IAM access policies applicable to this client application and IDP e.g. "app-bucket-write,app-bucket-list"` + defaultHelpPostfix(RolePolicy),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClaimName,
			Description: `JWT canned policy claim name` + defaultHelpPostfix(ClaimName),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         Scopes,
			Description: `Comma separated list of OpenID scopes for server, defaults to advertised scopes from discovery document e.g. "email,admin"` + defaultHelpPostfix(Scopes),
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         Vendor,
			Description: `Specify vendor type for vendor specific behavior to checking validity of temporary credentials and service accounts on MinIO` + defaultHelpPostfix(Vendor),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClaimUserinfo,
			Description: `Enable fetching claims from UserInfo Endpoint for authenticated user` + defaultHelpPostfix(ClaimUserinfo),
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         KeyCloakRealm,
			Description: `Specify Keycloak 'realm' name, only honored if vendor was set to 'keycloak' as value, if no realm is specified 'master' is default` + defaultHelpPostfix(KeyCloakRealm),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         KeyCloakAdminURL,
			Description: `Specify Keycloak 'admin' REST API endpoint e.g. http://localhost:8080/auth/admin/` + defaultHelpPostfix(KeyCloakAdminURL),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         RedirectURIDynamic,
			Description: `Enable 'Host' header based dynamic redirect URI` + defaultHelpPostfix(RedirectURIDynamic),
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         ClaimPrefix,
			Description: `[DEPRECATED use 'claim_name'] JWT claim namespace prefix e.g. "customer1/"` + defaultHelpPostfix(ClaimPrefix),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         RedirectURI,
			Description: `[DEPRECATED use env 'MINIO_BROWSER_REDIRECT_URL'] Configure custom redirect_uri for OpenID login flow callback` + defaultHelpPostfix(RedirectURI),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
