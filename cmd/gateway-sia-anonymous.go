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
	"io"
)

// AnonGetBucketInfo - Get bucket metadata from azure anonymously.
func (s *siaObjects) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	s.debugmsg("Gateway.AnonGetBucketInfo")
	return s.GetBucketInfo(bucket)
}

// AnonPutObject - SendPUT request without authentication.
// This is needed when clients send PUT requests on objects that can be uploaded without auth.
func (s *siaObjects) AnonPutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	s.debugmsg("Gateway.AnonPutObject")
	return s.PutObject(bucket, object, size, data, metadata, sha256sum)
}

// AnonGetObject - SendGET request without authentication.
// This is needed when clients send GET requests on objects that can be downloaded without auth.
func (s *siaObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	s.debugmsg("Gateway.AnonGetObject")
	return s.GetObject(bucket, object, startOffset, length, writer)
}

// AnonGetObjectInfo - Send HEAD request without authentication and convert the
// result to ObjectInfo.
func (s *siaObjects) AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	s.debugmsg("Gateway.AnonGetObjectInfo")
	return s.GetObjectInfo(bucket, object)
}

// AnonListObjects - Use Azure equivalent ListBlobs.
func (s *siaObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	s.debugmsg("Gateway.AnonListObjects")
	return s.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (s *siaObjects) AnonListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (result ListObjectsV2Info, err error) {
	s.debugmsg("Gateway.AnonListObjectsV2")
	return s.ListObjectsV2(bucket, prefix, continuationToken, fetchOwner, delimiter, maxKeys)
}
