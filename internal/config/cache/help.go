// Copyright (c) 2015-2023 MinIO, Inc.
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

package cache

import "github.com/minio/minio/internal/config"

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help - provides help for cache config
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Enable,
			Type:        "on|off",
			Description: "set to enable various caching optimizations" + defaultHelpPostfix(Enable),
			Optional:    true,
		},
		config.HelpKV{
			Key:         Endpoint,
			Type:        "string",
			Description: "remote endpoint where MinIO will cache GET/HEAD metadata values such as ETag, ModTime" + defaultHelpPostfix(Endpoint),
			Optional:    true,
		},
		config.HelpKV{
			Key:         ObjectSize,
			Type:        "string",
			Description: "maximum object size below which data is cached and fetched remotely from DRAM if possible" + defaultHelpPostfix(ObjectSize),
			Optional:    true,
		},
	}
)
