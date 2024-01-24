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

package batch

import "github.com/minio/minio/internal/config"

// Help template for batch feature.
var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help provides help for config values
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         ReplicationWorkersWait,
			Description: `maximum sleep duration between objects to slow down batch replication operation` + defaultHelpPostfix(ReplicationWorkersWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         KeyRotationWorkersWait,
			Description: `maximum sleep duration between objects to slow down batch keyrotation operation` + defaultHelpPostfix(KeyRotationWorkersWait),
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         ExpirationWorkersWait,
			Description: "maximum sleep duration between objects to slow down batch expiration operation" + defaultHelpPostfix(ExpirationWorkersWait),
			Optional:    true,
			Type:        "duration",
		},
	}
)
