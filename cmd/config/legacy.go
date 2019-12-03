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
 *
 */

package config

import "github.com/minio/minio/pkg/auth"

//// One time migration code section

// SetCredentials - One time migration code needed, for migrating from older config to new for server credentials.
func SetCredentials(c Config, cred auth.Credentials) {
	creds, err := auth.CreateCredentials(cred.AccessKey, cred.SecretKey)
	if err != nil {
		return
	}
	if !creds.IsValid() {
		return
	}
	c[CredentialsSubSys][Default] = KVS{
		KV{
			Key:   AccessKey,
			Value: cred.AccessKey,
		},
		KV{
			Key:   SecretKey,
			Value: cred.SecretKey,
		},
	}
}

// SetRegion - One time migration code needed, for migrating from older config to new for server Region.
func SetRegion(c Config, name string) {
	if name == "" {
		return
	}
	c[RegionSubSys][Default] = KVS{
		KV{
			Key:   RegionName,
			Value: name,
		},
	}
}
