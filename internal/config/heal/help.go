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

package heal

import "github.com/minio/minio/internal/config"

// Help template for caching feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Bitrot,
			Description: `perform bitrot scan on drives when checking objects during scanner` + defaultHelpPostfix(Bitrot),
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         Sleep,
			Description: `maximum sleep duration between objects to slow down heal operation` + defaultHelpPostfix(Sleep),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         IOCount,
			Description: `maximum IO requests allowed between objects to slow down heal operation` + defaultHelpPostfix(IOCount),
			Optional:    true,
			Type:        "int",
		},
		config.HelpKV{
			Key:         DriveWorkers,
			Description: `the number of workers per drive to heal a new disk replacement` + defaultHelpPostfix(DriveWorkers),
			Optional:    true,
			Type:        "int",
		},
	}
)
