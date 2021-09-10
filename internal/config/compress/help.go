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

package compress

import "github.com/minio/minio/internal/config"

// Help template for compress feature.
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Extensions,
			Description: `comma separated file extensions e.g. ".txt,.log,.csv"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         MimeTypes,
			Description: `comma separated wildcard mime-types e.g. "text/*,application/json,application/xml"`,
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
