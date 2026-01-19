// Copyright (c) 2015-2023 MinIO, Inc.
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

package cmd

import (
	"bytes"
	"context"
	"maps"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

type fanOutOptions struct {
	Kind     crypto.Type
	KeyID    string
	Key      []byte
	KmsCtx   kms.Context
	Checksum *hash.Checksum
	MD5Hex   string
}

// fanOutPutObject takes an input source reader and fans out multiple PUT operations
// based on the incoming fan-out request, a context cancellation by the caller
// would ensure all fan-out operations are canceled.
func fanOutPutObject(ctx context.Context, bucket string, objectAPI ObjectLayer, fanOutEntries []minio.PutObjectFanOutEntry, fanOutBuf []byte, opts fanOutOptions) ([]ObjectInfo, []error) {
	errs := make([]error, len(fanOutEntries))
	objInfos := make([]ObjectInfo, len(fanOutEntries))

	var wg sync.WaitGroup
	for i, req := range fanOutEntries {
		wg.Add(1)
		go func(idx int, req minio.PutObjectFanOutEntry) {
			defer wg.Done()

			objInfos[idx] = ObjectInfo{Name: req.Key}

			hopts := hash.Options{
				Size:       int64(len(fanOutBuf)),
				MD5Hex:     opts.MD5Hex,
				SHA256Hex:  "",
				ActualSize: -1,
				DisableMD5: true,
			}
			hr, err := hash.NewReaderWithOpts(ctx, bytes.NewReader(fanOutBuf), hopts)
			if err != nil {
				errs[idx] = err
				return
			}

			reader := NewPutObjReader(hr)
			defer func() {
				if err := reader.Close(); err != nil {
					errs[idx] = err
				}
				if err := hr.Close(); err != nil {
					errs[idx] = err
				}
			}()

			userDefined := make(map[string]string, len(req.UserMetadata))
			maps.Copy(userDefined, req.UserMetadata)

			tgs, err := tags.NewTags(req.UserTags, true)
			if err != nil {
				errs[idx] = err
				return
			}

			userDefined[xhttp.AmzObjectTagging] = tgs.String()

			if opts.Kind != nil {
				encrd, objectEncryptionKey, err := newEncryptReader(ctx, hr, opts.Kind, opts.KeyID, opts.Key, bucket, req.Key, userDefined, opts.KmsCtx)
				if err != nil {
					errs[idx] = err
					return
				}

				// do not try to verify encrypted content/
				hr, err = hash.NewReader(ctx, encrd, -1, "", "", -1)
				if err != nil {
					errs[idx] = err
					return
				}

				reader, err = reader.WithEncryption(hr, &objectEncryptionKey)
				if err != nil {
					errs[idx] = err
					return
				}
			}

			objInfo, err := objectAPI.PutObject(ctx, bucket, req.Key, reader, ObjectOptions{
				Versioned:        globalBucketVersioningSys.PrefixEnabled(bucket, req.Key),
				VersionSuspended: globalBucketVersioningSys.PrefixSuspended(bucket, req.Key),
				UserDefined:      userDefined,
			})
			if err != nil {
				errs[idx] = err
				return
			}
			objInfos[idx] = objInfo
		}(i, req)
	}
	wg.Wait()

	return objInfos, errs
}
