/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"io"
	"net/http"

	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
)

type DummyObjectLayer struct{}

func (api *DummyObjectLayer) Shutdown(context.Context) (err error) {
	return
}

func (api *DummyObjectLayer) StorageInfo(context.Context) (si StorageInfo) {
	return
}

func (api *DummyObjectLayer) MakeBucketWithLocation(ctx context.Context, bucket string, location string) (err error) {
	return
}

func (api *DummyObjectLayer) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	return
}

func (api *DummyObjectLayer) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	return
}

func (api *DummyObjectLayer) DeleteBucket(ctx context.Context, bucket string) (err error) {
	return
}

func (api *DummyObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	return
}

func (api *DummyObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	return
}

func (api *DummyObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lock LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	return
}

func (api *DummyObjectLayer) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) (err error) {
	return
}

func (api *DummyObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return
}

func (api *DummyObjectLayer) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return
}

func (api *DummyObjectLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	return
}

func (api *DummyObjectLayer) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	return
}

func (api *DummyObjectLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	return
}

func (api *DummyObjectLayer) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string, opts ObjectOptions) (uploadID string, err error) {
	return
}

func (api *DummyObjectLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (info PartInfo, err error) {
	return
}

func (api *DummyObjectLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *hash.Reader, opts ObjectOptions) (info PartInfo, err error) {
	return
}

func (api *DummyObjectLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	return
}

func (api *DummyObjectLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	return
}

func (api *DummyObjectLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart) (objInfo ObjectInfo, err error) {
	return
}

func (api *DummyObjectLayer) ReloadFormat(ctx context.Context, dryRun bool) (err error) {
	return
}

func (api *DummyObjectLayer) HealFormat(ctx context.Context, dryRun bool) (item madmin.HealResultItem, err error) {
	return
}

func (api *DummyObjectLayer) HealBucket(ctx context.Context, bucket string, dryRun bool) (items []madmin.HealResultItem, err error) {
	return
}

func (api *DummyObjectLayer) HealObject(ctx context.Context, bucket, object string, dryRun bool) (item madmin.HealResultItem, err error) {
	return
}

func (api *DummyObjectLayer) ListBucketsHeal(ctx context.Context) (buckets []BucketInfo, err error) {
	return
}

func (api *DummyObjectLayer) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (info ListObjectsInfo, err error) {
	return
}

func (api *DummyObjectLayer) SetBucketPolicy(context.Context, string, *policy.Policy) (err error) {
	return
}

func (api *DummyObjectLayer) GetBucketPolicy(context.Context, string) (bucketPolicy *policy.Policy, err error) {
	return
}

func (api *DummyObjectLayer) RefreshBucketPolicy(context.Context, string) (err error) {
	return
}

func (api *DummyObjectLayer) DeleteBucketPolicy(context.Context, string) (err error) {
	return
}

func (api *DummyObjectLayer) IsNotificationSupported() (b bool) {
	return
}

func (api *DummyObjectLayer) IsEncryptionSupported() (b bool) {
	return
}

func (api *DummyObjectLayer) IsCompressionSupported() (b bool) {
	return
}
