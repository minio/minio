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
	"fmt"
	"testing"
)

func Benchmark_bucketMetacache_findCache(b *testing.B) {
	bm := newBucketMetacache("", false)
	const elements = 50000
	const paths = 100
	if elements%paths != 0 {
		b.Fatal("elements must be divisible by the number of paths")
	}
	var pathNames [paths]string
	for i := range pathNames[:] {
		pathNames[i] = fmt.Sprintf("prefix/%d", i)
	}
	for i := range elements {
		bm.findCache(listPathOptions{
			ID:           mustGetUUID(),
			Bucket:       "",
			BaseDir:      pathNames[i%paths],
			Prefix:       "",
			FilterPrefix: "",
			Marker:       "",
			Limit:        0,
			AskDisks:     "strict",
			Recursive:    false,
			Separator:    slashSeparator,
			Create:       true,
		})
	}
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		bm.findCache(listPathOptions{
			ID:           mustGetUUID(),
			Bucket:       "",
			BaseDir:      pathNames[i%paths],
			Prefix:       "",
			FilterPrefix: "",
			Marker:       "",
			Limit:        0,
			AskDisks:     "strict",
			Recursive:    false,
			Separator:    slashSeparator,
			Create:       true,
		})
	}
}
