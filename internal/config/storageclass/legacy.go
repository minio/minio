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

package storageclass

import (
	"github.com/minio/minio/internal/config"
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
