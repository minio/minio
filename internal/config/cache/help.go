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

package cache

import "github.com/minio/minio/internal/config"

// Help template for caching feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Drives,
			Description: `comma separated mountpoints e.g. "/optane1,/optane2"` + defaultHelpPostfix(Drives),
			Type:        "csv",
		},
		config.HelpKV{
			Key:         Expiry,
			Description: `cache expiry duration in days` + defaultHelpPostfix(Expiry),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         Quota,
			Description: `limit cache drive usage in percentage` + defaultHelpPostfix(Quota),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         Exclude,
			Description: `exclude cache for following patterns e.g. "bucket/*.tmp,*.exe"` + defaultHelpPostfix(Exclude),
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         After,
			Description: `minimum number of access before caching an object` + defaultHelpPostfix(After),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         WatermarkLow,
			Description: `% of cache use at which to stop cache eviction` + defaultHelpPostfix(WatermarkLow),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         WatermarkHigh,
			Description: `% of cache use at which to start cache eviction` + defaultHelpPostfix(WatermarkHigh),
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         Range,
			Description: `set to "on" or "off" caching of independent range requests per object` + defaultHelpPostfix(Range),
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         Commit,
			Description: `set to control cache commit behavior` + defaultHelpPostfix(Commit),
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
