// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package kms

import (
	"context"

	"github.com/minio/kes"
)

// KeyManager is the generic interface that handles KMS key operations
type KeyManager interface {
	// CreateKey creates a new key at the KMS with the given key ID.
	CreateKey(ctx context.Context, keyID string) error

	// DeleteKey deletes a key at the KMS with the given key ID.
	// Please note that is a dangerous operation.
	// Once a key has been deleted all data that has been encrypted with it cannot be decrypted
	// anymore, and therefore, is lost.
	DeleteKey(ctx context.Context, keyID string) error

	// ListKeys List all key names that match the specified pattern. In particular,
	// the pattern * lists all keys.
	ListKeys(ctx context.Context, pattern string) (*kes.KeyIterator, error)

	// ImportKey imports a cryptographic key into the KMS.
	ImportKey(ctx context.Context, keyID string, bytes []byte) error

	// EncryptKey Encrypts and authenticates a (small) plaintext with the cryptographic key
	// The plaintext must not exceed 1 MB
	EncryptKey(keyID string, plaintext []byte, context Context) ([]byte, error)
}
