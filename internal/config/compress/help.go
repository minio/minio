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
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	Help = config.HelpKVS{
		config.HelpKV{
			Key:         config.Enable,
			Description: "Enable or disable object compression",
			Type:        "on|off",
			Optional:    true,
			Sensitive:   false,
		},
		config.HelpKV{
			Key:         Extensions,
			Description: `comma separated file extensions` + defaultHelpPostfix(Extensions),
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         MimeTypes,
			Description: `comma separated wildcard mime-types` + defaultHelpPostfix(MimeTypes),
			Optional:    true,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         AllowEncrypted,
			Description: `enable 'encryption' along with compression`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
