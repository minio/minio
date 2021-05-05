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

package condition

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

func getValuesByKey(m map[string][]string, key string) []string {
	if values, found := m[http.CanonicalHeaderKey(key)]; found {
		return values
	}
	return m[key]
}

// Splits an incoming path into bucket and object components.
func path2BucketAndObject(path string) (bucket, object string) {
	// Skip the first element if it is '/', split the rest.
	path = strings.TrimPrefix(path, "/")
	pathComponents := strings.SplitN(path, "/", 2)

	// Save the bucket and object extracted from path.
	switch len(pathComponents) {
	case 1:
		bucket = pathComponents[0]
	case 2:
		bucket = pathComponents[0]
		object = pathComponents[1]
	}
	return bucket, object
}

// Value - is enum type of string, int or bool.
type Value struct {
	t reflect.Kind
	s string
	i int
	b bool
}

// GetBool - gets stored bool value.
func (v Value) GetBool() (bool, error) {
	var err error

	if v.t != reflect.Bool {
		err = fmt.Errorf("not a bool Value")
	}

	return v.b, err
}

// GetInt - gets stored int value.
func (v Value) GetInt() (int, error) {
	var err error

	if v.t != reflect.Int {
		err = fmt.Errorf("not a int Value")
	}

	return v.i, err
}

// GetString - gets stored string value.
func (v Value) GetString() (string, error) {
	var err error

	if v.t != reflect.String {
		err = fmt.Errorf("not a string Value")
	}

	return v.s, err
}

// GetType - gets enum type.
func (v Value) GetType() reflect.Kind {
	return v.t
}

// MarshalJSON - encodes Value to JSON data.
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.t {
	case reflect.String:
		return json.Marshal(v.s)
	case reflect.Int:
		return json.Marshal(v.i)
	case reflect.Bool:
		return json.Marshal(v.b)
	}

	return nil, fmt.Errorf("unknown value kind %v", v.t)
}

// StoreBool - stores bool value.
func (v *Value) StoreBool(b bool) {
	*v = Value{t: reflect.Bool, b: b}
}

// StoreInt - stores int value.
func (v *Value) StoreInt(i int) {
	*v = Value{t: reflect.Int, i: i}
}

// StoreString - stores string value.
func (v *Value) StoreString(s string) {
	*v = Value{t: reflect.String, s: s}
}

// String - returns string representation of value.
func (v Value) String() string {
	switch v.t {
	case reflect.String:
		return v.s
	case reflect.Int:
		return strconv.Itoa(v.i)
	case reflect.Bool:
		return strconv.FormatBool(v.b)
	}

	return ""
}

// UnmarshalJSON - decodes JSON data.
func (v *Value) UnmarshalJSON(data []byte) error {
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		v.StoreBool(b)
		return nil
	}

	var i int
	if err := json.Unmarshal(data, &i); err == nil {
		v.StoreInt(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v.StoreString(s)
		return nil
	}

	return fmt.Errorf("unknown json data '%v'", data)
}

// NewBoolValue - returns new bool value.
func NewBoolValue(b bool) Value {
	value := &Value{}
	value.StoreBool(b)
	return *value
}

// NewIntValue - returns new int value.
func NewIntValue(i int) Value {
	value := &Value{}
	value.StoreInt(i)
	return *value
}

// NewStringValue - returns new string value.
func NewStringValue(s string) Value {
	value := &Value{}
	value.StoreString(s)
	return *value
}
