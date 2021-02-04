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

package storageclass

import (
	"github.com/minio/minio/cmd/config"
)

// SetStorageClass - One time migration code needed, for migrating from older config to new for StorageClass.
func SetStorageClass(s config.Config, cfg Config) {
	if len(cfg.Standard.String()) == 0 && len(cfg.RRS.String()) == 0 {
		// Do not enable storage-class if no settings found.
		return
	}
	s[config.StorageClassSubSys][config.Default] = config.KVS{
		config.KV{
			Key:   ClassStandard,
			Value: cfg.Standard.String(),
		},
		config.KV{
			Key:   ClassRRS,
			Value: cfg.RRS.String(),
		},
	}
}
