/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package schema

import (
	"testing"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
)

func TestTreeSet(t *testing.T) {
	a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		element   *Element
		expectErr bool
	}{
		{"A", a, false},
		{"A.B", b, false},
		{"A.B.C", c, false},
		{"B.C", nil, true},      // error: parent B does not exist
		{"A.B.C.AA", nil, true}, // error: parent A.B.C is not group element
	}

	root := NewTree()
	for i, testCase := range testCases {
		err := root.Set(testCase.name, testCase.element)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			if testCase.expectErr {
				t.Fatalf("case %v: err: expected: <error>, got: <nil>", i+1)
			} else {
				t.Fatalf("case %v: err: expected: <nil>, got: %v", i+1, err)
			}
		}
	}
}

func TestTreeGet(t *testing.T) {
	a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	root := NewTree()
	if err := root.Set("A", a); err != nil {
		t.Fatal(err)
	}
	if err := root.Set("A.B", b); err != nil {
		t.Fatal(err)
	}
	if err := root.Set("A.B.C", c); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name            string
		expectedElement *Element
		expectedFound   bool
	}{
		{"A", a, true},
		{"A.B", b, true},
		{"A.B.C", c, true},
		{"B", nil, false},
		{"A.B.C.AA", nil, false},
	}

	for i, testCase := range testCases {
		element, found := root.Get(testCase.name)

		if element != testCase.expectedElement {
			t.Fatalf("case %v: element: expected: %v, got: %v", i+1, testCase.expectedElement, element)
		}

		if found != testCase.expectedFound {
			t.Fatalf("case %v: found: expected: %v, got: %v", i+1, testCase.expectedFound, found)
		}
	}
}

func TestTreeDelete(t *testing.T) {
	testCases := []struct {
		name          string
		expectedFound bool
	}{
		{"A", false},
		{"A.B", false},
		{"A.B.C", false},
	}

	for i, testCase := range testCases {
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		root := NewTree()
		if err := root.Set("A", a); err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}
		if err := root.Set("A.B", b); err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}
		if err := root.Set("A.B.C", c); err != nil {
			t.Fatalf("case %v: %v", i+1, err)
		}

		root.Delete(testCase.name)
		_, found := root.Get(testCase.name)

		if found != testCase.expectedFound {
			t.Fatalf("case %v: found: expected: %v, got: %v", i+1, testCase.expectedFound, found)
		}
	}
}

func TestTreeToParquetSchema(t *testing.T) {
	case1Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case1Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
	}

	case2Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case2Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
	}

	case3Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP_KEY_VALUE), nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case3Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
	}

	case4Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := case4Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
	}

	case5Root := NewTree()
	{
		a, err := NewElement("a", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := NewElement("b", parquet.FieldRepetitionType_OPTIONAL, nil, nil, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewElement("c", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := case5Root.Set("A", a); err != nil {
			t.Fatal(err)
		}
		if err := case5Root.Set("A.B", b); err != nil {
			t.Fatal(err)
		}
		if err := case5Root.Set("A.B.C", c); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		tree      *Tree
		expectErr bool
	}{
		{case1Root, true}, // err: A: group element must have children
		{case2Root, true}, // err: A: ConvertedType INT_8 must have Type value
		{case3Root, true}, // err: A: unsupported ConvertedType MAP_KEY_VALUE
		{case4Root, false},
		{case5Root, false},
	}

	for i, testCase := range testCases {
		_, _, err := testCase.tree.ToParquetSchema()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			if testCase.expectErr {
				t.Fatalf("case %v: err: expected: <error>, got: <nil>", i+1)
			} else {
				t.Fatalf("case %v: err: expected: <nil>, got: %v", i+1, err)
			}
		}
	}
}

func TestTreeToParquetSchemaOfList(t *testing.T) {
	case1Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case1Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
	}

	case2Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case2Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}
	}

	case3Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case3Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case3Root.Set("Names.a", a); err != nil {
			t.Fatal(err)
		}
	}

	case4Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("LIST", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case4Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case4Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}
	}

	case5Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("list", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case5Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case5Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}
	}

	case6Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case6Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case6Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}
	}

	case7Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("Names.list.a", a); err != nil {
			t.Fatal(err)
		}
	}

	case8Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		element, err := NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("Names.list.element", element); err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("Names.list.a", a); err != nil {
			t.Fatal(err)
		}
	}

	case9Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		element, err := NewElement("ELEMENT", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("Names.list.element", element); err != nil {
			t.Fatal(err)
		}
	}

	case10Root := NewTree()
	{
		names, err := NewElement("names", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		list, err := NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		element, err := NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("Names", names); err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("Names.list", list); err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("Names.list.element", element); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		tree      *Tree
		expectErr bool
	}{
		{case1Root, true}, // err: Names: type must be nil for LIST ConvertedType
		{case2Root, true}, // err: Names: children must have one element only for LIST ConvertedType
		{case3Root, true}, // err: Names: missing group element 'list' for LIST ConvertedType
		{case4Root, true}, // err: Names.list: name must be 'list'
		{case5Root, true}, // err: Names.list: repetition type must be REPEATED type
		{case6Root, true}, // err: Names.list.element: not found
		{case7Root, true}, // err: Names.list.element: not found
		{case8Root, true}, // err: Names.list.element: not found
		{case9Root, true}, // err: Names.list.element: name must be 'element'
		{case10Root, false},
	}

	for i, testCase := range testCases {
		_, _, err := testCase.tree.ToParquetSchema()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			if testCase.expectErr {
				t.Fatalf("case %v: err: expected: <error>, got: <nil>", i+1)
			} else {
				t.Fatalf("case %v: err: expected: <nil>, got: %v", i+1, err)
			}
		}
	}
}

func TestTreeToParquetSchemaOfMap(t *testing.T) {
	case1Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case1Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
	}

	case2Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case2Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}
	}

	case3Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case3Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case3Root.Set("NameMap.a", a); err != nil {
			t.Fatal(err)
		}
	}

	case4Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("keyValue", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case4Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case4Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}
	}

	case5Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case5Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case5Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}
	}

	case6Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case6Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case6Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}
	}

	case7Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		b, err := NewElement("b", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		c, err := NewElement("c", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("NameMap.key_value.a", a); err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("NameMap.key_value.b", b); err != nil {
			t.Fatal(err)
		}

		if err := case7Root.Set("NameMap.key_value.c", c); err != nil {
			t.Fatal(err)
		}
	}

	case8Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case8Root.Set("NameMap.key_value.a", a); err != nil {
			t.Fatal(err)
		}
	}

	case9Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("KEY", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case9Root.Set("NameMap.key_value.key", key); err != nil {
			t.Fatal(err)
		}
	}

	case10Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case10Root.Set("NameMap.key_value.key", key); err != nil {
			t.Fatal(err)
		}
	}

	case11Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		a, err := NewElement("a", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case11Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case11Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case11Root.Set("NameMap.key_value.key", key); err != nil {
			t.Fatal(err)
		}

		if err := case11Root.Set("NameMap.key_value.a", a); err != nil {
			t.Fatal(err)
		}
	}

	case12Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		value, err := NewElement("VALUE", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case12Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case12Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case12Root.Set("NameMap.key_value.key", key); err != nil {
			t.Fatal(err)
		}

		if err := case12Root.Set("NameMap.key_value.value", value); err != nil {
			t.Fatal(err)
		}
	}

	case13Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case13Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case13Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case13Root.Set("NameMap.key_value.key", key); err != nil {
			t.Fatal(err)
		}
	}

	case14Root := NewTree()
	{
		nameMap, err := NewElement("nameMap", parquet.FieldRepetitionType_REQUIRED,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		keyValue, err := NewElement("key_value", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		key, err := NewElement("key", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		value, err := NewElement("value", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := case14Root.Set("NameMap", nameMap); err != nil {
			t.Fatal(err)
		}

		if err := case14Root.Set("NameMap.key_value", keyValue); err != nil {
			t.Fatal(err)
		}

		if err := case14Root.Set("NameMap.key_value.key", key); err != nil {
			t.Fatal(err)
		}

		if err := case13Root.Set("NameMap.key_value.value", value); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		tree      *Tree
		expectErr bool
	}{
		{case1Root, true},  // err: NameMap: type must be nil for MAP ConvertedType
		{case2Root, true},  // err: NameMap: children must have one element only for MAP ConvertedType
		{case3Root, true},  // err: NameMap: missing group element 'key_value' for MAP ConvertedType
		{case4Root, true},  // err: NameMap.key_value: name must be 'key_value'
		{case5Root, true},  // err: NameMap.key_value: repetition type must be REPEATED type
		{case6Root, true},  // err: NameMap.key_value: children must have 'key' and optionally 'value' elements for MAP ConvertedType
		{case7Root, true},  // err: NameMap.key_value: children must have 'key' and optionally 'value' elements for MAP ConvertedType
		{case8Root, true},  // err: NameMap.key_value: missing 'key' element for MAP ConvertedType
		{case9Root, true},  // err: NameMap.key_value.key: name must be 'key'
		{case10Root, true}, // err: NameMap.key_value: repetition type must be REQUIRED type
		{case11Root, true}, // err: NameMap.key_value: second element must be 'value' element for MAP ConvertedType
		{case12Root, true}, // err: NameMap.key_value.value: name must be 'value'
		{case13Root, false},
		{case14Root, false},
	}

	for i, testCase := range testCases {
		_, _, err := testCase.tree.ToParquetSchema()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			if testCase.expectErr {
				t.Fatalf("case %v: err: expected: <error>, got: <nil>", i+1)
			} else {
				t.Fatalf("case %v: err: expected: <nil>, got: %v", i+1, err)
			}
		}
	}
}
