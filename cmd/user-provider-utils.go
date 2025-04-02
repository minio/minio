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

	"github.com/minio/madmin-go/v3"
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
