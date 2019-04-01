package cmd

import (
	"github.com/minio/minio/pkg/iam/ldap"
	"github.com/minio/minio/pkg/iam/openid"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	config "github.com/minio/minio/pkg/server-config"
)

func (s serverConfig) SetPolicyOPAConfig(opaArgs iampolicy.OpaArgs) {
	s[config.PolicyOPASubSys][config.Default] = config.KVS{
		config.State: func() string {
			if opaArgs.URL == nil {
				return config.StateDisabled
			}
			return config.StateEnabled
		}(),
		iampolicy.OpaURL: func() string {
			if opaArgs.URL != nil {
				return opaArgs.URL.String()
			}
			return ""
		}(),
		iampolicy.OpaAuthToken: opaArgs.AuthToken,
	}
}

func (s serverConfig) SetIdentityLDAP(ldapArgs ldap.Config) {
	s[config.IdentityLDAPSubSys][config.Default] = config.KVS{
		config.State: func() string {
			if !ldapArgs.IsEnabled {
				return config.StateDisabled
			}
			return config.StateEnabled
		}(),
		ldap.ServerAddr:         ldapArgs.ServerAddr,
		ldap.STSExpiry:          ldapArgs.STSExpiryDuration,
		ldap.UsernameFormat:     ldapArgs.UsernameFormat,
		ldap.GroupSearchFilter:  ldapArgs.GroupSearchFilter,
		ldap.GroupNameAttribute: ldapArgs.GroupNameAttribute,
		ldap.GroupSearchBaseDN:  ldapArgs.GroupSearchBaseDN,
	}
}

func (s serverConfig) SetIdentityOpenID(openidArgs openid.JWKSArgs) {
	s[config.IdentityOpenIDSubSys][config.Default] = config.KVS{
		config.State: func() string {
			if openidArgs.URL == nil {
				return config.StateDisabled
			}
			return config.StateEnabled
		}(),

		openid.JwksURL: func() string {
			if openidArgs.URL != nil {
				return openidArgs.URL.String()
			}
			return ""
		}(),
	}
}
