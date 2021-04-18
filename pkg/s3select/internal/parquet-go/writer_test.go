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
	"os"
	"testing"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/data"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/schema"
)

func TestWriterWrite(t *testing.T) {
	schemaTree := schema.NewTree()
	{
		one, err := schema.NewElement("one", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		two, err := schema.NewElement("two", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		three, err := schema.NewElement("three", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BOOLEAN), nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := schemaTree.Set("one", one); err != nil {
			t.Fatal(err)
		}
		if err := schemaTree.Set("two", two); err != nil {
			t.Fatal(err)
		}
		if err := schemaTree.Set("three", three); err != nil {
			t.Fatal(err)
		}
	}

	file, err := os.Create("test.parquet")
	if err != nil {
		t.Fatal(err)
	}

	writer, err := NewWriter(file, schemaTree, 100)
	if err != nil {
		t.Fatal(err)
	}

	oneColumn := data.NewColumn(parquet.Type_INT32)
	oneColumn.AddInt32(100, 0, 0)

	twoColumn := data.NewColumn(parquet.Type_BYTE_ARRAY)
	twoColumn.AddByteArray([]byte("foo"), 0, 0)

	threeColumn := data.NewColumn(parquet.Type_BOOLEAN)
	threeColumn.AddBoolean(true, 0, 0)

	record := map[string]*data.Column{
		"one":   oneColumn,
		"two":   twoColumn,
		"three": threeColumn,
	}

	err = writer.Write(record)
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriterWriteJSON(t *testing.T) {
	schemaTree := schema.NewTree()
	{
		one, err := schema.NewElement("one", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		two, err := schema.NewElement("two", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		three, err := schema.NewElement("three", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BOOLEAN), nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := schemaTree.Set("one", one); err != nil {
			t.Fatal(err)
		}
		if err := schemaTree.Set("two", two); err != nil {
			t.Fatal(err)
		}
		if err := schemaTree.Set("three", three); err != nil {
			t.Fatal(err)
		}
	}

	file, err := os.Create("test.parquet")
	if err != nil {
		t.Fatal(err)
	}

	writer, err := NewWriter(file, schemaTree, 100)
	if err != nil {
		t.Fatal(err)
	}

	record := `{"one": 100, "two": "foo", "three": true}`
	err = writer.WriteJSON([]byte(record))
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
}
