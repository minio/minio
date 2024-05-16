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

package drive

import "github.com/minio/minio/internal/config"

var (
	// MaxTimeout is the max timeout for drive
	MaxTimeout = "max_timeout"

	// HelpDrive is help for drive
	HelpDrive = config.HelpKVS{
		config.HelpKV{
			Key:         MaxTimeout,
			Type:        "string",
			Description: "set per call max_timeout for the drive, defaults to 30 seconds",
			Optional:    true,
		},
	}
)
