/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package target

import (
	"errors"
	"github.com/minio/minio/pkg/event"
)

// ErrLimitExceeded error is sent when the maximum limit is reached.
var ErrLimitExceeded = errors.New("[Store] The maximum limit reached")

// ErrNoSuchKey error is sent in Get when the key is not found.
var ErrNoSuchKey = errors.New("[Store] No such key found")

// Store - To persist the events.
type Store interface {
	Put(event event.Event) error
	Get(key string) (event.Event, error)
	ListAll() []string
	Del(key string)
	Open() error
}
