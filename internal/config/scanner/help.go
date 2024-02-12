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

package scanner

import "github.com/minio/minio/internal/config"

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Speed,
			Description: `customize scanner speed (default|slowest|slow|fast|fastest)` + defaultHelpPostfix(Speed),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         ExcessVersions,
			Description: `alert per object beyond this many versions` + defaultHelpPostfix(ExcessVersions),
			Optional:    true,
			Type:        "int",
		},
		config.HelpKV{
			Key:         ExcessFolders,
			Description: `alert beyond this many sub-folders per folder in an erasure set` + defaultHelpPostfix(ExcessFolders),
			Optional:    true,
			Type:        "int",
		},
	}
)
