package cmd

import (
	"fmt"
	"testing"
)

func Benchmark_bucketMetacache_findCache(b *testing.B) {
	bm := newBucketMetacache("")
	const elements = 50000
	const paths = 100
	if elements%paths != 0 {
		b.Fatal("elements must be divisible by the number of paths")
	}
	var pathNames [paths]string
	for i := range pathNames[:] {
		pathNames[i] = fmt.Sprintf("prefix/%d", i)
	}
	for i := 0; i < elements; i++ {
		bm.findCache(listPathOptions{
			ID:           mustGetUUID(),
			Bucket:       "",
			BaseDir:      pathNames[i%paths],
			Prefix:       "",
			FilterPrefix: "",
			Marker:       "",
			Limit:        0,
			AskDisks:     0,
			Recursive:    false,
			Separator:    slashSeparator,
			Create:       true,
			CurrentCycle: uint64(i),
			OldestCycle:  uint64(i - 1),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.findCache(listPathOptions{
			ID:           mustGetUUID(),
			Bucket:       "",
			BaseDir:      pathNames[i%paths],
			Prefix:       "",
			FilterPrefix: "",
			Marker:       "",
			Limit:        0,
			AskDisks:     0,
			Recursive:    false,
			Separator:    slashSeparator,
			Create:       true,
			CurrentCycle: uint64(i % elements),
			OldestCycle:  uint64(0),
		})
	}
}
