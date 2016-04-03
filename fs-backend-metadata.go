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

package main

import (
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/quick"
)

var multipartsMetadataPath string

// SetFSMultipartsMetadataPath - set custom multiparts session metadata path.
func setFSMultipartsMetadataPath(metadataPath string) {
	multipartsMetadataPath = metadataPath
}

// saveMultipartsSession - save multiparts.
func saveMultipartsSession(mparts multiparts) *probe.Error {
	qc, err := quick.New(mparts)
	if err != nil {
		return err.Trace()
	}
	if err := qc.Save(multipartsMetadataPath); err != nil {
		return err.Trace()
	}
	return nil
}

// loadMultipartsSession load multipart session file.
func loadMultipartsSession() (*multiparts, *probe.Error) {
	mparts := &multiparts{}
	mparts.Version = "1"
	mparts.ActiveSession = make(map[string]*multipartSession)
	qc, err := quick.New(mparts)
	if err != nil {
		return nil, err.Trace()
	}
	if err := qc.Load(multipartsMetadataPath); err != nil {
		return nil, err.Trace()
	}
	return qc.Data().(*multiparts), nil
}
