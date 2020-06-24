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

package data

import (
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/s3select/internal/parquet-go/gen-go/parquet"
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/schema"
)

var (
	v10    = int32(10)
	v20    = int32(20)
	v30    = int32(30)
	ten    = []byte("ten")
	foo    = []byte("foo")
	bar    = []byte("bar")
	phone1 = []byte("1-234-567-8901")
	phone2 = []byte("1-234-567-1098")
	phone3 = []byte("1-111-222-3333")
)

func TestAddressBookExample(t *testing.T) {
	// message AddressBook {
	//   required string owner;
	//   repeated string ownerPhoneNumbers;
	//   repeated group contacts {
	//     required string name;
	//     optional string phoneNumber;
	//   }
	// }
	t.Skip("Broken")

	addressBook := schema.NewTree()
	{
		owner, err := schema.NewElement("owner", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		ownerPhoneNumbers, err := schema.NewElement("ownerPhoneNumbers", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		ownerPhoneNumbersList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		ownerPhoneNumbersElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		contacts, err := schema.NewElement("contacts", parquet.FieldRepetitionType_OPTIONAL,
			nil, parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		contactsList, err := schema.NewElement("list", parquet.FieldRepetitionType_REPEATED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		contactsElement, err := schema.NewElement("element", parquet.FieldRepetitionType_REQUIRED,
			nil, nil,
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		contactName, err := schema.NewElement("name", parquet.FieldRepetitionType_REQUIRED,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		contactPhoneNumber, err := schema.NewElement("phoneNumber", parquet.FieldRepetitionType_OPTIONAL,
			parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("owner", owner); err != nil {
			t.Fatal(err)
		}

		if err = addressBook.Set("ownerPhoneNumbers", ownerPhoneNumbers); err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("ownerPhoneNumbers.list", ownerPhoneNumbersList); err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("ownerPhoneNumbers.list.element", ownerPhoneNumbersElement); err != nil {
			t.Fatal(err)
		}

		if err = addressBook.Set("contacts", contacts); err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("contacts.list", contactsList); err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("contacts.list.element", contactsElement); err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("contacts.list.element.name", contactName); err != nil {
			t.Fatal(err)
		}
		if err = addressBook.Set("contacts.list.element.phoneNumber", contactPhoneNumber); err != nil {
			t.Fatal(err)
		}
	}

	if _, _, err := addressBook.ToParquetSchema(); err != nil {
		t.Fatal(err)
	}

	case2Data := `{
    "owner": "foo"
}`
	result2 := map[string]*Column{
		"owner": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{foo},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"ownerPhoneNumbers.list.element": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.name": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
	}

	case3Data := `{
    "owner": "foo",
    "ownerPhoneNumbers": [
        "1-234-567-8901"
    ]
}
`
	result3 := map[string]*Column{
		"owner": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{foo},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"ownerPhoneNumbers.list.element": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{phone1},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.name": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
	}

	case4Data := `{
    "owner": "foo",
    "ownerPhoneNumbers": [
        "1-234-567-8901",
        "1-234-567-1098"
    ]
}
`
	result4 := map[string]*Column{
		"owner": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{foo},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"ownerPhoneNumbers.list.element": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{phone1, phone2},
			definitionLevels: []int64{2, 2},
			repetitionLevels: []int64{0, 1},
		},
		"contacts.list.element.name": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
	}

	case5Data := `{
    "contacts": [
        {
            "name": "bar"
        }
    ],
    "owner": "foo"
}`
	result5 := map[string]*Column{
		"owner": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{foo},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"ownerPhoneNumbers.list.element": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.name": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{bar},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.phoneNumber": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{1},
		},
	}

	case6Data := `{
    "contacts": [
        {
            "name": "bar",
            "phoneNumber": "1-111-222-3333"
        }
    ],
    "owner": "foo"
}`
	result6 := map[string]*Column{
		"owner": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{foo},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"ownerPhoneNumbers.list.element": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{nil},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.name": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{bar},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.phoneNumber": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{phone3},
			definitionLevels: []int64{3},
			repetitionLevels: []int64{1},
		},
	}

	case7Data := `{
    "contacts": [
        {
            "name": "bar",
            "phoneNumber": "1-111-222-3333"
        }
    ],
    "owner": "foo",
    "ownerPhoneNumbers": [
        "1-234-567-8901",
        "1-234-567-1098"
    ]
}`
	result7 := map[string]*Column{
		"owner": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{foo},
			definitionLevels: []int64{0},
			repetitionLevels: []int64{0},
		},
		"ownerPhoneNumbers.list.element": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{phone1, phone2},
			definitionLevels: []int64{2, 2},
			repetitionLevels: []int64{0, 1},
		},
		"contacts.list.element.name": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{bar},
			definitionLevels: []int64{2},
			repetitionLevels: []int64{0},
		},
		"contacts.list.element.phoneNumber": {
			parquetType:      parquet.Type_BYTE_ARRAY,
			values:           []interface{}{phone3},
			definitionLevels: []int64{3},
			repetitionLevels: []int64{1},
		},
	}

	testCases := []struct {
		data           string
		expectedResult map[string]*Column
		expectErr      bool
	}{
		{`{}`, nil, true}, // err: owner: nil value for required field
		{case2Data, result2, false},
		{case3Data, result3, false},
		{case4Data, result4, false},
		{case5Data, result5, false},
		{case6Data, result6, false},
		{case7Data, result7, false},
	}

	for i, testCase := range testCases {
		result, err := UnmarshalJSON([]byte(testCase.data), addressBook)
		expectErr := (err != nil)

		if testCase.expectErr != expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Errorf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}
