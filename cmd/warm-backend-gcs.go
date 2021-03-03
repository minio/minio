/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

package cmd

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/minio/minio/pkg/madmin"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
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
func (gcs *warmBackendGCS) Put(ctx context.Context, key string, data io.Reader, length int64) error {
	object := gcs.client.Bucket(gcs.Bucket).Object(gcs.getDest(key))
	//TODO: set storage class
	w := object.NewWriter(ctx)
	if gcs.StorageClass != "" {
		w.ObjectAttrs.StorageClass = gcs.StorageClass
	}
	if _, err := io.Copy(w, data); err != nil {
		return gcsToObjectError(err, gcs.Bucket, key)
	}

	return w.Close()
}

func (gcs *warmBackendGCS) Get(ctx context.Context, key string, opts warmBackendGetOpts) (r io.ReadCloser, err error) {
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

func (gcs *warmBackendGCS) Remove(ctx context.Context, key string) error {
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

func newWarmBackendGCS(conf madmin.TierGCS) (*warmBackendGCS, error) {
	credsJSON, err := conf.GetCredentialJSON()
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(context.Background(), option.WithCredentialsJSON(credsJSON), option.WithScopes(storage.ScopeReadWrite))
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
