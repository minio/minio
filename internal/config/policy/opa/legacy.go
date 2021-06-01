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

package opa

import (
	"github.com/minio/minio/internal/config"
)

// Legacy OPA envs
const (
	EnvIamOpaURL       = "MINIO_IAM_OPA_URL"
	EnvIamOpaAuthToken = "MINIO_IAM_OPA_AUTHTOKEN"
)

// SetPolicyOPAConfig - One time migration code needed, for migrating from older config to new for PolicyOPAConfig.
func SetPolicyOPAConfig(s config.Config, opaArgs Args) {
	if opaArgs.URL == nil || opaArgs.URL.String() == "" {
		// Do not enable if opaArgs was empty.
		return
	}
	s[config.PolicyOPASubSys][config.Default] = config.KVS{
		config.KV{
			Key:   URL,
			Value: opaArgs.URL.String(),
		},
		config.KV{
			Key:   AuthToken,
			Value: opaArgs.AuthToken,
		},
	}
}
