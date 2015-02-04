/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package minioapi

import (
	"encoding/xml"
)

const (
	MAX_OBJECT_LIST = 1000
)

type ObjectListResponse struct {
	XMLName     xml.Name `xml:"ListBucketResult" json:"-"`
	Name        string
	Marker      string
	MaxKeys     int
	IsTruncated bool
	Contents    []*Item `xml:"Contents",innerxml`
}

type BucketListResponse struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult" json:"-"`
	Owner   Owner
	Buckets struct {
		Bucket []*Bucket
	} `xml:"Buckets",innerxml` // Buckets are nested
}

type Bucket struct {
	Name         string
	CreationDate string
}

type Item struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	StorageClass string
	Owner        Owner
}

type Owner struct {
	ID          string
	DisplayName string
}

var unimplementedBucketResourceNames = map[string]bool{
	"acl":            true,
	"lifecycle":      true,
	"policy":         true,
	"location":       true,
	"logging":        true,
	"notification":   true,
	"versions":       true,
	"requestPayment": true,
	"versioning":     true,
	"website":        true,
	"uploads":        true,
}

var unimplementedObjectResourceNames = map[string]bool{
	"uploadId": true,
	"acl":      true,
	"torrent":  true,
	"uploads":  true,
}
