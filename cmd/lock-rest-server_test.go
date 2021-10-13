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

package cmd

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"testing"

	"github.com/minio/minio/internal/dsync"
)

func BenchmarkLockArgs(b *testing.B) {
	args := dsync.LockArgs{
		Owner:     "minio",
		UID:       "uid",
		Source:    "lockArgs.go",
		Quorum:    3,
		Resources: []string{"obj.txt"},
	}

	argBytes, err := args.MarshalMsg(nil)
	if err != nil {
		b.Fatal(err)
	}

	req := &http.Request{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Body = ioutil.NopCloser(bytes.NewReader(argBytes))
		getLockArgs(req)
	}
}

func BenchmarkLockArgsOld(b *testing.B) {
	values := url.Values{}
	values.Set("owner", "minio")
	values.Set("uid", "uid")
	values.Set("source", "lockArgs.go")
	values.Set("quorum", "3")

	req := &http.Request{
		Form: values,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Body = ioutil.NopCloser(bytes.NewReader([]byte(`obj.txt`)))
		getLockArgsOld(req)
	}
}

func getLockArgsOld(r *http.Request) (args dsync.LockArgs, err error) {
	values := r.Form
	quorum, err := strconv.Atoi(values.Get("quorum"))
	if err != nil {
		return args, err
	}

	args = dsync.LockArgs{
		Owner:  values.Get("onwer"),
		UID:    values.Get("uid"),
		Source: values.Get("source"),
		Quorum: quorum,
	}

	var resources []string
	bio := bufio.NewScanner(r.Body)
	for bio.Scan() {
		resources = append(resources, bio.Text())
	}

	if err := bio.Err(); err != nil {
		return args, err
	}

	sort.Strings(resources)
	args.Resources = resources
	return args, nil
}
