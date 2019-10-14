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
	s[config.IdentityOpenIDSubSys][config.Default] = config.KVS{
		config.State: func() string {
			if cfg.JWKS.URL == nil {
				return config.StateOff
			}
			if cfg.JWKS.URL.String() == "" {
				return config.StateOff
			}
			return config.StateOn
		}(),
		config.Comment: "Settings for OpenID, after migrating config",
		JwksURL: func() string {
			if cfg.JWKS.URL != nil {
				return cfg.JWKS.URL.String()
			}
			return ""
		}(),
		ConfigURL:   "",
		ClaimPrefix: "",
	}
}
