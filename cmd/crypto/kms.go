// MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"github.com/minio/minio/pkg/kms"
)

// Context is a list of key-value pairs cryptographically
// associated with a certain object.
type Context = kms.Context

// KMS represents an active and authenticted connection
// to a Key-Management-Service. It supports generating
// data key generation and unsealing of KMS-generated
// data keys.
type KMS = kms.KMS
