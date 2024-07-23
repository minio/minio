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

package subnet

import "github.com/minio/minio/internal/config"

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// HelpSubnet - provides help for subnet api key config
	HelpSubnet = config.HelpKVS{
		config.HelpKV{
			Key:         config.License,
			Type:        "string",
			Description: "Enterprise license for the cluster" + defaultHelpPostfix(config.License),
			Optional:    true,
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         config.APIKey,
			Type:        "string",
			Description: "Enterprise license API key for the cluster" + defaultHelpPostfix(config.APIKey),
			Optional:    true,
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         config.Proxy,
			Type:        "string",
			Description: "HTTP(s) proxy URL to use for connecting to SUBNET" + defaultHelpPostfix(config.Proxy),
			Optional:    true,
			Sensitive:   true,
		},
	}
)
