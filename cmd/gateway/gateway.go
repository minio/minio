/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gateway

import (
	// Import all gateways please keep the order

	// NAS
	_ "github.com/minio/minio/cmd/gateway/nas"

	// S3
	_ "github.com/minio/minio/cmd/gateway/s3"
	// gateway functionality is frozen, no new gateways are being implemented
	// or considered for upstream inclusion at this point in time. if needed
	// please keep a fork of the project.
)
