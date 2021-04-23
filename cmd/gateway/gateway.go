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

package gateway

import (
	// Import all gateways please keep the order

	// NAS
	_ "github.com/minio/minio/cmd/gateway/nas"

	// Azure
	_ "github.com/minio/minio/cmd/gateway/azure"

	// S3
	_ "github.com/minio/minio/cmd/gateway/s3"

	// HDFS
	_ "github.com/minio/minio/cmd/gateway/hdfs"

	// GCS (use only if you must, GCS already supports S3 API)
	_ "github.com/minio/minio/cmd/gateway/gcs"
	// gateway functionality is frozen, no new gateways are being implemented
	// or considered for upstream inclusion at this point in time. if needed
	// please keep a fork of the project.
)
