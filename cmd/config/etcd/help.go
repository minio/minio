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
			Description: `Comma separated list of etcd endpoints eg: "http://localhost:2379"`,
			Type:        "csv",
		},
		config.HelpKV{
			Key:         PathPrefix,
			Description: `Default etcd path prefix to populate all IAM assets eg: "customer/"`,
			Type:        "path",
		},
		config.HelpKV{
			Key:         CoreDNSPath,
			Description: `Default etcd path location to populate bucket DNS srv records eg: "/skydns"`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         ClientCert,
			Description: `Etcd client cert for mTLS authentication`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         ClientCertKey,
			Description: `Etcd client cert key for mTLS authentication`,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: "A comment to describe the etcd settings",
			Optional:    true,
			Type:        "sentence",
		},
	}
)
