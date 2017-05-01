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
func (l *gcsGateway) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (l *gcsGateway) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return []BucketInfo{}, traceError(NotImplemented{})
}

// HealObject - Not relevant.
func (l *gcsGateway) HealObject(bucket string, object string) (int, int, error) {
	return 0, 0, traceError(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (l *gcsGateway) ListObjectsHeal(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, traceError(NotImplemented{})
}

// ListUploadsHeal - Not relevant.
func (l *gcsGateway) ListUploadsHeal(bucket string, prefix string, marker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, traceError(NotImplemented{})
}
