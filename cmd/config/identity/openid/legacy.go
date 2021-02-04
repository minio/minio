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

package openid

import "github.com/minio/minio/cmd/config"

// Legacy envs
const (
	EnvIamJwksURL = "MINIO_IAM_JWKS_URL"
)

// SetIdentityOpenID - One time migration code needed, for migrating from older config to new for OpenIDConfig.
func SetIdentityOpenID(s config.Config, cfg Config) {
	if cfg.JWKS.URL == nil || cfg.JWKS.URL.String() == "" {
		// No need to save not-enabled settings in new config.
		return
	}
	s[config.IdentityOpenIDSubSys][config.Default] = config.KVS{
		config.KV{
			Key:   JwksURL,
			Value: cfg.JWKS.URL.String(),
		},
		config.KV{
			Key:   ConfigURL,
			Value: "",
		},
		config.KV{
			Key:   ClaimPrefix,
			Value: "",
		},
	}
}
