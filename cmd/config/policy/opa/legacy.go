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

package opa

import (
	"github.com/minio/minio/cmd/config"
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
