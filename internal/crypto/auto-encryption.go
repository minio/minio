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

package crypto

import (
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

const (
	// EnvKMSAutoEncryption is the environment variable used to en/disable
	// SSE-S3 auto-encryption. SSE-S3 auto-encryption, if enabled,
	// requires a valid KMS configuration and turns any non-SSE-C
	// request into an SSE-S3 request.
	// If present EnvAutoEncryption must be either "on" or "off".
	EnvKMSAutoEncryption = "MINIO_KMS_AUTO_ENCRYPTION"
)

// LookupAutoEncryption returns true if and only if
// the MINIO_KMS_AUTO_ENCRYPTION env. variable is
// set to "on".
func LookupAutoEncryption() bool {
	auto, _ := config.ParseBool(env.Get(EnvKMSAutoEncryption, config.EnableOff))
	return auto
}
