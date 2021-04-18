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
	"fmt"
	"strings"
)

// Record - ordered parquet record.
type Record struct {
	nameList     []string
	nameValueMap map[string]Value
}

// String - returns string representation of this record.
func (r *Record) String() string {
	values := []string{}
	r.Range(func(name string, value Value) bool {
		values = append(values, fmt.Sprintf("%v:%v", name, value))
		return true
	})

	return "map[" + strings.Join(values, " ") + "]"
}

func (r *Record) set(name string, value Value) {
	r.nameValueMap[name] = value
}

// Get - returns Value of name.
func (r *Record) Get(name string) (Value, bool) {
	value, ok := r.nameValueMap[name]
	return value, ok
}

// Range - calls f sequentially for each name and value present in the record. If f returns false, range stops the iteration.
func (r *Record) Range(f func(name string, value Value) bool) {
	for _, name := range r.nameList {
		value, ok := r.nameValueMap[name]
		if !ok {
			continue
		}

		if !f(name, value) {
			break
		}
	}
}

func newRecord(nameList []string) *Record {
	return &Record{
		nameList:     nameList,
		nameValueMap: make(map[string]Value),
	}
}
