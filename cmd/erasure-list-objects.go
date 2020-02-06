/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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

	"github.com/minio/minio/pkg/madmin"
)

// ListObjectVersions - This is not implemented, look for erasure-zones.ListObjectVersions()
func (er erasureObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi ListObjectVersionsInfo, e error) {
	return loi, NotImplemented{}
}

// ListObjectsV2 - This is not implemented/needed anymore, look for erasure-zones.ListObjectsV2()
func (er erasureObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi ListObjectsV2Info, e error) {
	return loi, NotImplemented{}
}

// ListObjects - This is not implemented/needed anymore, look for erasure-zones.ListObjects()
func (er erasureObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	return loi, NotImplemented{}
}

// ListBucketsHeal - This is not implemented/needed anymore, look for erasure-zones.ListBucketHeal()
func (er erasureObjects) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	return nil, NotImplemented{}
}

// ListObjectsHeal - This is not implemented, look for erasure-zones.ListObjectsHeal()
func (er erasureObjects) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	return ListObjectsInfo{}, NotImplemented{}
}

// HealObjects - This is not implemented/needed anymore, look for erasure-zones.HealObjects()
func (er erasureObjects) HealObjects(ctx context.Context, bucket, prefix string, _ madmin.HealOpts, _ HealObjectFn) (e error) {
	return NotImplemented{}
}

// Walk - This is not implemented/needed anymore, look for erasure-zones.Walk()
func (er erasureObjects) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	return NotImplemented{}
}
