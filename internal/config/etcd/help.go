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

package etcd

import "github.com/minio/minio/internal/config"

// etcd config documented in default config
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Endpoints,
			Description: `comma separated list of etcd endpoints e.g. "http://localhost:2379"`,
			Type:        "csv",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         PathPrefix,
			Description: `namespace prefix to isolate tenants e.g. "customer1/"`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         CoreDNSPath,
			Description: `shared bucket DNS records, default is "/skydns"`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         ClientCert,
			Description: `client cert for mTLS authentication`,
			Optional:    true,
			Type:        "path",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         ClientCertKey,
			Description: `client cert key for mTLS authentication`,
			Optional:    true,
			Type:        "path",
			Sensitive:   true,
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
