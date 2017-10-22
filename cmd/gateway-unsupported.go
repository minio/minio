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

import (
	"github.com/minio/minio/pkg/hash"
)

type gatewayUnsupported struct{}

// CopyObjectPart - Not implemented.
func (a gatewayUnsupported) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string,
	partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, traceError(NotImplemented{})
}

// HealBucket - Not relevant.
func (a gatewayUnsupported) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (a gatewayUnsupported) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return nil, traceError(NotImplemented{})
}

// HealObject - Not relevant.
func (a gatewayUnsupported) HealObject(bucket, object string) (int, int, error) {
	return 0, 0, traceError(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (a gatewayUnsupported) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	return loi, traceError(NotImplemented{})
}

// ListUploadsHeal - Not relevant.
func (a gatewayUnsupported) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	return lmi, traceError(NotImplemented{})
}

// AnonListObjects - List objects anonymously
func (a gatewayUnsupported) AnonListObjects(bucket string, prefix string, marker string, delimiter string,
	maxKeys int) (loi ListObjectsInfo, err error) {
	return loi, traceError(NotImplemented{})
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (a gatewayUnsupported) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi ListObjectsV2Info, err error) {
	return loi, traceError(NotImplemented{})
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (a gatewayUnsupported) AnonGetBucketInfo(bucket string) (bi BucketInfo, err error) {
	return bi, traceError(NotImplemented{})
}

// AnonPutObject creates a new object anonymously with the incoming data,
func (a gatewayUnsupported) AnonPutObject(bucket, object string, data *hash.Reader,
	metadata map[string]string) (ObjectInfo, error) {
	return ObjectInfo{}, traceError(NotImplemented{})
}
