// Copyright (c) 2015-2024 MinIO, Inc.
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

import "github.com/minio/minio/internal/config"

// Help template for storageclass feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ClassStandard,
			Description: `set the parity count for default standard storage class` + defaultHelpPostfix(ClassStandard),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClassRRS,
			Description: `set the parity count for reduced redundancy storage class` + defaultHelpPostfix(ClassRRS),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         Optimize,
			Description: `optimize parity calculation for standard storage class, set 'capacity' for capacity optimized (no additional parity)` + defaultHelpPostfix(Optimize),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
