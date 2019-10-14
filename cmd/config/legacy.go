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
	c[CredentialsSubSys][Default] = KVS{
		State:     StateOn,
		Comment:   "Settings for credentials, after migrating config",
		AccessKey: cred.AccessKey,
		SecretKey: cred.SecretKey,
	}
}

// SetRegion - One time migration code needed, for migrating from older config to new for server Region.
func SetRegion(c Config, name string) {
	c[RegionSubSys][Default] = KVS{
		RegionName: name,
		State:      StateOn,
		Comment:    "Settings for Region, after migrating config",
	}
}

// SetWorm - One time migration code needed, for migrating from older config to new for Worm mode.
func SetWorm(c Config, b bool) {
	// Set the new value.
	c[WormSubSys][Default] = KVS{
		State: func() string {
			if b {
				return StateOn
			}
			return StateOff
		}(),
		Comment: "Settings for WORM, after migrating config",
	}
}
