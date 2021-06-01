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

import "github.com/minio/minio/internal/config"

// Help template for storageclass feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ClassStandard,
			Description: `set the parity count for default standard storage class e.g. "EC:4"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClassRRS,
			Description: `set the parity count for reduced redundancy storage class e.g. "EC:2"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ClassDMA,
			Description: `enable O_DIRECT for both read and write, defaults to "write" e.g. "read+write"`,
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
