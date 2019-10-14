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

package openid

import "github.com/minio/minio/cmd/config"

// Help template for OpenID identity feature.
var (
	Help = config.HelpKV{
		ConfigURL:      `OpenID discovery documented endpoint. eg: "https://accounts.google.com/.well-known/openid-configuration"`,
		config.State:   "Indicates if OpenID identity is enabled or not",
		config.Comment: "A comment to describe the OpenID identity setting",
	}
)
