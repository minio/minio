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
	Help = config.HelpKV{
		Endpoints:      `(required) Comma separated list of etcd endpoints eg: "http://localhost:2379"`,
		CoreDNSPath:    `(optional) CoreDNS etcd path location to populate DNS srv records eg: "/skydns"`,
		ClientCert:     `(optional) Etcd client cert for mTLS authentication`,
		ClientCertKey:  `(optional) Etcd client cert key for mTLS authentication`,
		config.State:   "Indicates if etcd config is on or off",
		config.Comment: "A comment to describe the etcd settings",
	}
)
