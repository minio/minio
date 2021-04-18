// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
