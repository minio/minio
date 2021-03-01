/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"encoding/json"
	"testing"
	"time"
)

func TestXLV2Format(t *testing.T) {
	failOnErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	xl := xlMetaV2{}
	fi := FileInfo{
		Volume:           "volume",
		Name:             "object-name",
		VersionID:        "756100c6-b393-4981-928a-d49bbc164741",
		IsLatest:         true,
		Deleted:          false,
		TransitionStatus: "",
		DataDir:          "bffea160-ca7f-465f-98bc-9b4f1c3ba1ef",
		XLV1:             false,
		ModTime:          time.Now(),
		Size:             0,
		Mode:             0,
		Metadata:         nil,
		Parts:            nil,
		Erasure: ErasureInfo{
			Algorithm:    ReedSolomon.String(),
			DataBlocks:   4,
			ParityBlocks: 2,
			BlockSize:    10000,
			Index:        1,
			Distribution: []int{1, 2, 3, 4, 5, 6, 7, 8},
			Checksums: []ChecksumInfo{{
				PartNumber: 1,
				Algorithm:  HighwayHash256S,
				Hash:       nil,
			}},
		},
		MarkDeleted:                   false,
		DeleteMarkerReplicationStatus: "",
		VersionPurgeStatus:            "",
		Data:                          []byte("some object data"),
		NumVersions:                   1,
		SuccessorModTime:              time.Time{},
	}

	failOnErr(xl.AddVersion(fi))
	failOnErr(xl.AddVersion(fi))
	serialized, err := xl.AppendTo(nil)
	failOnErr(err)
	var xl2 xlMetaV2
	failOnErr(xl2.Load(serialized))
	b, err := json.MarshalIndent(xl2, "", "  ")
	failOnErr(err)
	t.Log(string(b))
	t.Log(len(xl2.data))
	t.Log(xl2.data.list())
	t.Log(string(xl2.data.find("bffea160-ca7f-465f-98bc-9b4f1c3ba1ef")))
	t.Log(string(xl.data.find("bffea160-ca7f-465f-98bc-9b4f1c3ba1ef")))
	xl2 = xlMetaV2{}
	failOnErr(xl2.Load(xlMetaV2TrimData(serialized)))
	t.Log(len(xl2.data))
	t.Log(xl2.data.list())
	t.Log(string(xl2.data.find("bffea160-ca7f-465f-98bc-9b4f1c3ba1ef")))
}
