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

// HealBucket - Not relevant.
func (a AzureObjects) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (a AzureObjects) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return nil, traceError(NotImplemented{})
}

// HealObject - Not relevant.
func (a AzureObjects) HealObject(bucket, object string) error {
	return traceError(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (a AzureObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, traceError(NotImplemented{})
}

// ListUploadsHeal - Not relevant.
func (a AzureObjects) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, traceError(NotImplemented{})
}
