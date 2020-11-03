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
 */

package dns

// Error - DNS related errors error.
type Error struct {
	Bucket string
	Err    error
}

// ErrInvalidBucketName for buckets with invalid name
type ErrInvalidBucketName Error

func (e ErrInvalidBucketName) Error() string {
	return "invalid bucket name error: " + e.Err.Error()
}
func (e Error) Error() string {
	return "dns related error: " + e.Err.Error()
}

// Store dns record store
type Store interface {
	Put(bucket string) error
	Get(bucket string) ([]SrvRecord, error)
	Delete(bucket string) error
	List() (map[string][]SrvRecord, error)
	DeleteRecord(record SrvRecord) error
	Close() error
	String() string
}
