/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package kv

import (
	"context"

	"github.com/minio/minio-go/v6/pkg/set"
)

// Backend simplefied KV store interface
type Backend interface {
	// Get get value for given key. Returns nil if key does not exist
	Get(ctx context.Context, key string) ([]byte, error)

	// Save saves given key/valye pair
	Save(ctx context.Context, key string, data []byte) error

	// Keys Returns all child keys of given basekey. The basekey is removed (KeyTransformDefault)
	Keys(ctx context.Context, basekey string) (set.StringSet, error)

	// KeysFilter Returns all child keys of given basekey and filters and transforms the key with given KeyTransform
	KeysFilter(ctx context.Context, basekey string, transform KeyTransform) (set.StringSet, error)

	// Delete deletes given key
	Delete(ctx context.Context, key string) error

	// Watch registers a watch event handler
	Watch(ctx context.Context, key string, handler EventHandler)

	// Returns additional backend information
	Info() string
}
