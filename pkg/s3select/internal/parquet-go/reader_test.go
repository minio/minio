/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package parquet

import (
	"io"
	"os"
	"testing"

	"github.com/minio/minio-go/v7/pkg/set"
)

func getReader(name string, offset int64, length int64) (io.ReadCloser, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if offset < 0 {
		offset = fi.Size() + offset
	}

	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	return file, nil
}

func TestReader(t *testing.T) {
	name := "example.parquet"
	reader, err := NewReader(
		func(offset, length int64) (io.ReadCloser, error) {
			return getReader(name, offset, length)
		},
		set.CreateStringSet("one", "two", "three"),
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedRecords := []string{
		`map[one:{-1 DOUBLE SchemaElement({Type:DOUBLE TypeLength:<nil> RepetitionType:OPTIONAL Name:one NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})} three:{true BOOLEAN SchemaElement({Type:BOOLEAN TypeLength:<nil> RepetitionType:OPTIONAL Name:three NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})} two:{[102 111 111] BYTE_ARRAY SchemaElement({Type:BYTE_ARRAY TypeLength:<nil> RepetitionType:OPTIONAL Name:two NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})}]`,
		`map[one:{<nil> DOUBLE SchemaElement({Type:DOUBLE TypeLength:<nil> RepetitionType:OPTIONAL Name:one NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})} three:{false BOOLEAN SchemaElement({Type:BOOLEAN TypeLength:<nil> RepetitionType:OPTIONAL Name:three NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})} two:{[98 97 114] BYTE_ARRAY SchemaElement({Type:BYTE_ARRAY TypeLength:<nil> RepetitionType:OPTIONAL Name:two NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})}]`,
		`map[one:{2.5 DOUBLE SchemaElement({Type:DOUBLE TypeLength:<nil> RepetitionType:OPTIONAL Name:one NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})} three:{true BOOLEAN SchemaElement({Type:BOOLEAN TypeLength:<nil> RepetitionType:OPTIONAL Name:three NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})} two:{[98 97 122] BYTE_ARRAY SchemaElement({Type:BYTE_ARRAY TypeLength:<nil> RepetitionType:OPTIONAL Name:two NumChildren:<nil> ConvertedType:<nil> Scale:<nil> Precision:<nil> FieldID:<nil> LogicalType:<nil>})}]`,
	}

	i := 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}

			break
		}

		if i == len(expectedRecords) {
			t.Errorf("read more than expected record count %v", len(expectedRecords))
		}

		if record.String() != expectedRecords[i] {
			t.Errorf("record%v: expected: %v, got: %v", i+1, expectedRecords[i], record.String())
		}

		i++
	}

	reader.Close()
}
