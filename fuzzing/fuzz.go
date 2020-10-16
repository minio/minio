// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in https://golang.org/LICENSE

// +build gofuzz


package fuzz

import (
	"bytes"
	csv "github.com/minio/minio/pkg/csvparser"
	"reflect"
)

func Fuzz(data []byte) int {
	buf := new(bytes.Buffer)

	for _, tt := range []csv.Reader{
		{},
		{Comma: ';'},
		{Comma: '\t'},
		{LazyQuotes: true},
		{TrimLeadingSpace: true},
		{Comment: '#'},
		{Comment: ';'},
	} {
		r := csv.NewReader(bytes.NewReader(data))
		r.Comma = tt.Comma
		r.Comment = tt.Comment
		r.LazyQuotes = tt.LazyQuotes
		r.TrimLeadingSpace = tt.TrimLeadingSpace

		records, err := r.ReadAll()
		if err != nil {
			continue
		}

		buf.Reset()
		w := csv.NewWriter(buf)
		w.Comma = tt.Comma
		err = w.WriteAll(records)
		if err != nil {
			return 0
		}

		r = csv.NewReader(buf)
		r.Comma = tt.Comma
		r.Comment = tt.Comment
		r.LazyQuotes = tt.LazyQuotes
		r.TrimLeadingSpace = tt.TrimLeadingSpace
		result, err := r.ReadAll()
		if err != nil {
			return 0
		}

		if !reflect.DeepEqual(records, result) {
			return 0
		}
	}

	return 1
}
