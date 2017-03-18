/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

import "io"

type s3Layer struct{}

func newS3Layer() (*s3Layer, error) {
	return &s3Layer{}, nil
}

func (l *s3Layer) Shutdown() error {
	panic("not implemented")
}

func (l *s3Layer) StorageInfo() StorageInfo {
	panic("not implemented")
}

func (l *s3Layer) MakeBucket(bucket string) error {
	panic("not implemented")
}

func (l *s3Layer) GetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) ListBuckets() (buckets []BucketInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) DeleteBucket(bucket string) error {
	panic("not implemented")
}

func (l *s3Layer) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) GetObject(bucket string, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	panic("not implemented")
}

func (l *s3Layer) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) DeleteObject(bucket string, object string) error {
	panic("not implemented")
}

func (l *s3Layer) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	panic("not implemented")
}

func (l *s3Layer) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) PutObjectPart(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (info PartInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	panic("not implemented")
}

func (l *s3Layer) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) HealBucket(bucket string) error {
	panic("not implemented")
}

func (l *s3Layer) ListBucketsHeal() (buckets []BucketInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) HealObject(bucket string, object string) error {
	panic("not implemented")
}

func (l *s3Layer) ListObjectsHeal(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	panic("not implemented")
}

func (l *s3Layer) ListUploadsHeal(bucket string, prefix string, marker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	panic("not implemented")
}

func (l *s3Layer) AnonGetObject(bucket string, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	panic("not implemented")
}

func (l *s3Layer) AnonGetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) SetBucketPolicies(string, []BucketAccessPolicy) error {
	panic("not implemented")
}

func (l *s3Layer) GetBucketPolicies(string) ([]BucketAccessPolicy, error) {
	panic("not implemented")
}

func (l *s3Layer) DeleteBucketPolicies(string) error {
	panic("not implemented")
}

func (l *s3Layer) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	panic("not implemented")
}

func (l *s3Layer) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	panic("not implemented")
}
