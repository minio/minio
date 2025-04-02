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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/minio/madmin-go/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	xioutil "github.com/minio/minio/internal/ioutil"
)

type warmBackendGCS struct {
	client       *storage.Client
	Bucket       string
	Prefix       string
	StorageClass string
}

func (gcs *warmBackendGCS) getDest(object string) string {
	destObj := object
	if gcs.Prefix != "" {
		destObj = fmt.Sprintf("%s/%s", gcs.Prefix, object)
	}
	return destObj
}

func (gcs *warmBackendGCS) PutWithMeta(ctx context.Context, key string, data io.Reader, length int64, meta map[string]string) (remoteVersionID, error) {
	object := gcs.client.Bucket(gcs.Bucket).Object(gcs.getDest(key))
	w := object.NewWriter(ctx)
	if gcs.StorageClass != "" {
		w.StorageClass = gcs.StorageClass
	}
	w.Metadata = meta
	if _, err := xioutil.Copy(w, data); err != nil {
		return "", gcsToObjectError(err, gcs.Bucket, key)
	}

	if _, err := xioutil.Copy(w, data); err != nil {
		return "", gcsToObjectError(err, gcs.Bucket, key)
	}

	return "", w.Close()
}

// FIXME: add support for remote version ID in GCS remote tier and remove this.
// Currently it's a no-op.
func (gcs *warmBackendGCS) Put(ctx context.Context, key string, data io.Reader, length int64) (remoteVersionID, error) {
	return gcs.PutWithMeta(ctx, key, data, length, map[string]string{})
}

func (gcs *warmBackendGCS) Get(ctx context.Context, key string, rv remoteVersionID, opts WarmBackendGetOpts) (r io.ReadCloser, err error) {
	// GCS storage decompresses a gzipped object by default and returns the data.
	// Refer to https://cloud.google.com/storage/docs/transcoding#decompressive_transcoding
	// Need to set `Accept-Encoding` header to `gzip` when issuing a GetObject call, to be able
	// to download the object in compressed state.
	// Calling ReadCompressed with true accomplishes that.
	object := gcs.client.Bucket(gcs.Bucket).Object(gcs.getDest(key)).ReadCompressed(true)

	r, err = object.NewRangeReader(ctx, opts.startOffset, opts.length)
	if err != nil {
		return nil, gcsToObjectError(err, gcs.Bucket, key)
	}
	return r, nil
}

func (gcs *warmBackendGCS) Remove(ctx context.Context, key string, rv remoteVersionID) error {
	err := gcs.client.Bucket(gcs.Bucket).Object(gcs.getDest(key)).Delete(ctx)
	return gcsToObjectError(err, gcs.Bucket, key)
}

func (gcs *warmBackendGCS) InUse(ctx context.Context) (bool, error) {
	it := gcs.client.Bucket(gcs.Bucket).Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    gcs.Prefix,
		Versions:  false,
	})
	pager := iterator.NewPager(it, 1, "")
	gcsObjects := make([]*storage.ObjectAttrs, 0)
	_, err := pager.NextPage(&gcsObjects)
	if err != nil {
		return false, gcsToObjectError(err, gcs.Bucket, gcs.Prefix)
	}
	if len(gcsObjects) > 0 {
		return true, nil
	}
	return false, nil
}

func newWarmBackendGCS(conf madmin.TierGCS, tier string) (*warmBackendGCS, error) {
	// Validation code
	if conf.Creds == "" {
		return nil, errors.New("empty credentials unsupported")
	}

	if conf.Bucket == "" {
		return nil, errors.New("no bucket name was provided")
	}

	credsJSON, err := conf.GetCredentialJSON()
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(context.Background(),
		option.WithCredentialsJSON(credsJSON),
		option.WithScopes(storage.ScopeReadWrite),
		option.WithUserAgent(fmt.Sprintf("gcs-tier-%s", tier)+SlashSeparator+ReleaseTag),
	)
	if err != nil {
		return nil, err
	}
	return &warmBackendGCS{client, conf.Bucket, conf.Prefix, conf.StorageClass}, nil
}

// Convert GCS errors to minio object layer errors.
func gcsToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	uploadID := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}
	if len(params) == 3 {
		uploadID = params[2]
	}

	// in some cases just a plain error is being returned
	switch err.Error() {
	case "storage: bucket doesn't exist":
		err = BucketNotFound{
			Bucket: bucket,
		}
		return err
	case "storage: object doesn't exist":
		if uploadID != "" {
			err = InvalidUploadID{
				UploadID: uploadID,
			}
		} else {
			err = ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
		}
		return err
	}

	googleAPIErr, ok := err.(*googleapi.Error)
	if !ok {
		// We don't interpret non MinIO errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	if len(googleAPIErr.Errors) == 0 {
		return err
	}

	reason := googleAPIErr.Errors[0].Reason
	message := googleAPIErr.Errors[0].Message

	switch reason {
	case "required":
		// Anonymous users does not have storage.xyz access to project 123.
		fallthrough
	case "keyInvalid":
		fallthrough
	case "forbidden":
		err = PrefixAccessDenied{
			Bucket: bucket,
			Object: object,
		}
	case "invalid":
		err = BucketNameInvalid{
			Bucket: bucket,
		}
	case "notFound":
		if object != "" {
			err = ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
			break
		}
		err = BucketNotFound{Bucket: bucket}
	case "conflict":
		if message == "You already own this bucket. Please select another name." {
			err = BucketAlreadyOwnedByYou{Bucket: bucket}
			break
		}
		if message == "Sorry, that name is not available. Please try a different one." {
			err = BucketAlreadyExists{Bucket: bucket}
			break
		}
		err = BucketNotEmpty{Bucket: bucket}
	}

	return err
}
