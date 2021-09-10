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

package config

import "github.com/minio/minio/internal/auth"

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
