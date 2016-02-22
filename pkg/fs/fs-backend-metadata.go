/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package fs

import (
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

var multipartsMetadataPath, bucketsMetadataPath string

// setFSBucketsMetadataPath - set fs buckets metadata path.
func setFSBucketsMetadataPath(metadataPath string) {
	bucketsMetadataPath = metadataPath
}

// SetFSMultipartsMetadataPath - set custom multiparts session
// metadata path.
func setFSMultipartsMetadataPath(metadataPath string) {
	multipartsMetadataPath = metadataPath
}

// saveMultipartsSession - save multiparts
func saveMultipartsSession(multiparts Multiparts) *probe.Error {
	qc, err := quick.New(multiparts)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(multipartsMetadataPath); err != nil {
		return err.Trace()
	}
	return nil
}

// saveBucketsMetadata - save metadata of all buckets
func saveBucketsMetadata(buckets Buckets) *probe.Error {
	qc, err := quick.New(buckets)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(bucketsMetadataPath); err != nil {
		return err.Trace()
	}
	return nil
}

// loadMultipartsSession load multipart session file
func loadMultipartsSession() (*Multiparts, *probe.Error) {
	multiparts := &Multiparts{}
	multiparts.Version = "1"
	multiparts.ActiveSession = make(map[string]*MultipartSession)
	qc, err := quick.New(multiparts)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(multipartsMetadataPath); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*Multiparts), nil
}

// loadBucketsMetadata load buckets metadata file
func loadBucketsMetadata() (*Buckets, *probe.Error) {
	buckets := &Buckets{}
	buckets.Version = "1"
	buckets.Metadata = make(map[string]*BucketMetadata)
	qc, err := quick.New(buckets)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(bucketsMetadataPath); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*Buckets), nil
}
