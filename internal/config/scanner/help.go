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
			Key:         Delay,
			Description: `scanner delay multiplier` + defaultHelpPostfix(Delay),
			Optional:    true,
			Type:        "float",
		},
		config.HelpKV{
			Key:         MaxWait,
			Description: `maximum wait time between operations` + defaultHelpPostfix(MaxWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         Cycle,
			Description: `time duration between scanner cycles` + defaultHelpPostfix(Cycle),
			Optional:    true,
			Type:        "duration",
		},
	}
)
