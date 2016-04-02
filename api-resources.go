/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import (
	"net/url"
	"strconv"
)

// parse bucket url queries
func getBucketResources(values url.Values) (prefix, marker, delimiter string, maxkeys int, encodingType string) {
	prefix = values.Get("prefix")
	marker = values.Get("marker")
	delimiter = values.Get("delimiter")
	maxkeys, _ = strconv.Atoi(values.Get("max-keys"))
	encodingType = values.Get("encoding-type")
	return
}

// part bucket url queries for ?uploads
func getBucketMultipartResources(values url.Values) (v BucketMultipartResourcesMetadata) {
	v.Prefix = values.Get("prefix")
	v.KeyMarker = values.Get("key-marker")
	v.MaxUploads, _ = strconv.Atoi(values.Get("max-uploads"))
	v.Delimiter = values.Get("delimiter")
	v.EncodingType = values.Get("encoding-type")
	v.UploadIDMarker = values.Get("upload-id-marker")
	return
}

// parse object url queries
func getObjectResources(values url.Values) (v ObjectResourcesMetadata) {
	v.UploadID = values.Get("uploadId")
	v.PartNumberMarker, _ = strconv.Atoi(values.Get("part-number-marker"))
	v.MaxParts, _ = strconv.Atoi(values.Get("max-parts"))
	v.EncodingType = values.Get("encoding-type")
	return
}

// get upload id.
func getUploadID(values url.Values) (uploadID string) {
	return getObjectResources(values).UploadID
}
