// MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

// KeyStore represents an active and authenticated connection
// to a Key-Value-Store that holds secret keys. It supports
// loading, storing and deleting such secret keys.
type KeyStore interface {

	// Load fetches the 256 bit key from the key store
	// that has been stored at the given path. It returns
	// an error if fetching the key fails or if there is
	// no key under the path.
	Load(path string) (key [32]byte, err error)

	// Store stores a 256 bit key at the key store
	// under the given path. It returns an error if
	// storing the key fails.
	Store(path string, key [32]byte) error

	// Delete deletes the key stored under the given
	// path.
	Delete(path string) error
}
