// Copyright (c) 2015-2022 MinIO, Inc.
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

package callhome

import "github.com/minio/minio/internal/config"

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// HelpCallhome - provides help for callhome config
	HelpCallhome = config.HelpKVS{
		config.HelpKV{
			Key:         Enable,
			Type:        "on|off",
			Description: "set to enable callhome" + defaultHelpPostfix(Enable),
			Optional:    true,
		},
		config.HelpKV{
			Key:         Frequency,
			Type:        "duration",
			Description: "time duration between callhome cycles e.g. 24h" + defaultHelpPostfix(Frequency),
			Optional:    true,
		},
	}
)
