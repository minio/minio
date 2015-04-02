// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package mxj

import (
	"encoding/json"
	"errors"
	"github.com/fatih/structs"
	"reflect"
)

// Create a new Map value from a structure.  Error returned if argument is not a structure
// or if there is a json.Marshal or json.Unmarshal error.
//	Only public structure fields are decoded in the Map value. Also, json.Marshal structure encoding rules
//	are followed for decoding the structure fields.
func NewMapStruct(structVal interface{}) (Map, error) {
	if !structs.IsStruct(structVal) {
		return nil, errors.New("NewMapStruct() error: argument is not type Struct")
	}
	return structs.Map(structVal), nil
}

// Marshal a map[string]interface{} into a structure referenced by 'structPtr'. Error returned
// if argument is not a pointer or if json.Unmarshal returns an error.
//	json.Unmarshal structure encoding rules are followed to encode public structure fields.
func (mv Map) Struct(structPtr interface{}) error {
	m := map[string]interface{}(mv)
	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	// should check that we're getting a pointer.
	if reflect.ValueOf(structPtr).Kind() != reflect.Ptr {
		return errors.New("mv.Struct() error: argument is not type Ptr")
	}
	return json.Unmarshal(j, structPtr)
}
