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

package cmd

import "github.com/minio/madmin-go"

// GatewayMinioSysTmp prefix is used in Azure/GCS gateway for save metadata sent by Initialize Multipart Upload API.
const (
	GatewayMinioSysTmp  = "minio.sys.tmp/"
	AzureBackendGateway = "azure"
	GCSBackendGateway   = "gcs"
	HDFSBackendGateway  = "hdfs"
	NASBackendGateway   = "nas"
	S3BackendGateway    = "s3"
)

// Gateway represents a gateway backend.
type Gateway interface {
	// Name returns the unique name of the gateway.
	Name() string

	// NewGatewayLayer returns a new  ObjectLayer.
	NewGatewayLayer(creds madmin.Credentials) (ObjectLayer, error)
}
