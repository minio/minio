/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package etcd

import "github.com/minio/minio/cmd/config"

// etcd config documented in default config
var (
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         Endpoints,
			Description: `comma separated list of etcd endpoints e.g. "http://localhost:2379"`,
			Type:        "csv",
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
		},
		config.HelpKV{
			Key:         ClientCertKey,
			Description: `client cert key for mTLS authentication`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)
