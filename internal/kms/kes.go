// Copyright (c) 2015-2021 MinIO, Inc.
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
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/minio/kms-go/kes"
	"github.com/minio/madmin-go/v3"
)

type kesConn struct {
	defaultKeyID string
	client       *kes.Client
}

func (c *kesConn) Version(ctx context.Context) (string, error) {
	return c.client.Version(ctx)
}

func (c *kesConn) APIs(ctx context.Context) ([]madmin.KMSAPI, error) {
	APIs, err := c.client.APIs(ctx)
	if err != nil {
		if errors.Is(err, kes.ErrNotAllowed) {
			return nil, ErrPermission
		}
		return nil, Error{
			Code:    http.StatusInternalServerError,
			APICode: "kms:InternalError",
			Err:     "failed to list KMS APIs",
			Cause:   err,
		}
	}

	list := make([]madmin.KMSAPI, 0, len(APIs))
	for _, api := range APIs {
		list = append(list, madmin.KMSAPI{
			Method:  api.Method,
			Path:    api.Path,
			MaxBody: api.MaxBody,
			Timeout: int64(api.Timeout.Truncate(time.Second).Seconds()),
		})
	}
	return list, nil
}

// Stat returns the current KES status containing a
// list of KES endpoints and the default key ID.
func (c *kesConn) Status(ctx context.Context) (map[string]madmin.ItemState, error) {
	if len(c.client.Endpoints) == 1 {
		if _, err := c.client.Status(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			if errors.Is(err, kes.ErrNotAllowed) {
				return nil, ErrPermission
			}

			return map[string]madmin.ItemState{
				c.client.Endpoints[0]: madmin.ItemOffline,
			}, nil
		}
		return map[string]madmin.ItemState{
			c.client.Endpoints[0]: madmin.ItemOnline,
		}, nil
	}

	type Result struct {
		Endpoint  string
		ItemState madmin.ItemState
	}

	var wg sync.WaitGroup
	results := make([]Result, len(c.client.Endpoints))
	for i := range c.client.Endpoints {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			client := kes.Client{
				Endpoints:  []string{c.client.Endpoints[i]},
				HTTPClient: c.client.HTTPClient,
			}

			var item madmin.ItemState
			if _, err := client.Status(ctx); err == nil {
				item = madmin.ItemOnline
			} else {
				item = madmin.ItemOffline
			}
			results[i] = Result{
				Endpoint:  c.client.Endpoints[i],
				ItemState: item,
			}
		}(i)
	}
	wg.Wait()

	status := make(map[string]madmin.ItemState, len(results))
	for _, r := range results {
		if r.ItemState == madmin.ItemOnline {
			status[r.Endpoint] = madmin.ItemOnline
		} else {
			status[r.Endpoint] = madmin.ItemOffline
		}
	}
	return status, nil
}

func (c *kesConn) ListKeys(ctx context.Context, req *ListRequest) ([]madmin.KMSKeyInfo, string, error) {
	names, continueAt, err := c.client.ListKeys(ctx, req.Prefix, req.Limit)
	if err != nil {
		return nil, "", err
	}
	keyInfos := make([]madmin.KMSKeyInfo, len(names))
	for i := range names {
		keyInfos[i].Name = names[i]
	}
	return keyInfos, continueAt, nil
}

// CreateKey tries to create a new key at the KMS with the
// given key ID.
//
// If the a key with the same keyID already exists then
// CreateKey returns kes.ErrKeyExists.
func (c *kesConn) CreateKey(ctx context.Context, req *CreateKeyRequest) error {
	if err := c.client.CreateKey(ctx, req.Name); err != nil {
		if errors.Is(err, kes.ErrKeyExists) {
			return ErrKeyExists
		}
		if errors.Is(err, kes.ErrNotAllowed) {
			return ErrPermission
		}
		return errKeyCreationFailed(err)
	}
	return nil
}

// DeleteKey deletes a key at the KMS with the given key ID.
// Please note that is a dangerous operation.
// Once a key has been deleted all data that has been encrypted with it cannot be decrypted
// anymore, and therefore, is lost.
func (c *kesConn) DeleteKey(ctx context.Context, req *DeleteKeyRequest) error {
	if err := c.client.DeleteKey(ctx, req.Name); err != nil {
		if errors.Is(err, kes.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if errors.Is(err, kes.ErrNotAllowed) {
			return ErrPermission
		}
		return errKeyDeletionFailed(err)
	}
	return nil
}

// GenerateKey generates a new data encryption key using
// the key at the KES server referenced by the key ID.
//
// The default key ID will be used if keyID is empty.
//
// The context is associated and tied to the generated DEK.
// The same context must be provided when the generated
// key should be decrypted.
func (c *kesConn) GenerateKey(ctx context.Context, req *GenerateKeyRequest) (DEK, error) {
	aad, err := req.AssociatedData.MarshalText()
	if err != nil {
		return DEK{}, err
	}

	name := req.Name
	if name == "" {
		name = c.defaultKeyID
	}

	dek, err := c.client.GenerateKey(ctx, name, aad)
	if err != nil {
		if errors.Is(err, kes.ErrKeyNotFound) {
			return DEK{}, ErrKeyNotFound
		}
		if errors.Is(err, kes.ErrNotAllowed) {
			return DEK{}, ErrPermission
		}
		return DEK{}, errKeyGenerationFailed(err)
	}
	return DEK{
		KeyID:      name,
		Plaintext:  dek.Plaintext,
		Ciphertext: dek.Ciphertext,
	}, nil
}

// ImportKey imports a cryptographic key into the KMS.
func (c *kesConn) ImportKey(ctx context.Context, keyID string, bytes []byte) error {
	return c.client.ImportKey(ctx, keyID, &kes.ImportKeyRequest{
		Key: bytes,
	})
}

// EncryptKey Encrypts and authenticates a (small) plaintext with the cryptographic key
// The plaintext must not exceed 1 MB
func (c *kesConn) EncryptKey(keyID string, plaintext []byte, ctx Context) ([]byte, error) {
	ctxBytes, err := ctx.MarshalText()
	if err != nil {
		return nil, err
	}
	return c.client.Encrypt(context.Background(), keyID, plaintext, ctxBytes)
}

// DecryptKey decrypts the ciphertext with the key at the KES
// server referenced by the key ID. The context must match the
// context value used to generate the ciphertext.
func (c *kesConn) Decrypt(ctx context.Context, req *DecryptRequest) ([]byte, error) {
	aad, err := req.AssociatedData.MarshalText()
	if err != nil {
		return nil, err
	}

	plaintext, err := c.client.Decrypt(context.Background(), req.Name, req.Ciphertext, aad)
	if err != nil {
		if errors.Is(err, kes.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		if errors.Is(err, kes.ErrDecrypt) {
			return nil, ErrDecrypt
		}
		if errors.Is(err, kes.ErrNotAllowed) {
			return nil, ErrPermission
		}
		return nil, errDecryptionFailed(err)
	}
	return plaintext, nil
}

// MAC generates the checksum of the given req.Message using the key
// with the req.Name at the KMS.
func (c *kesConn) MAC(ctx context.Context, req *MACRequest) ([]byte, error) {
	mac, err := c.client.HMAC(context.Background(), req.Name, req.Message)
	if err != nil {
		if errors.Is(err, kes.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		if errors.Is(err, kes.ErrNotAllowed) {
			return nil, ErrPermission
		}
		if kErr, ok := err.(kes.Error); ok && kErr.Status() == http.StatusNotImplemented {
			return nil, ErrNotSupported
		}
	}
	return mac, nil
}
