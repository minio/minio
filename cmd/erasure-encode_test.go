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
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/dustin/go-humanize"
)

type badDisk struct{ StorageAPI }

func (a badDisk) String() string {
	return "bad-disk"
}

func (a badDisk) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	return errFaultyDisk
}

func (a badDisk) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	return nil, errFaultyDisk
}

func (a badDisk) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {
	return errFaultyDisk
}

func (badDisk) Hostname() string {
	return ""
}

const oneMiByte = 1 * humanize.MiByte

var erasureEncodeTests = []struct {
	dataBlocks                   int
	onDisks, offDisks            int
	blocksize, data              int64
	offset                       int
	algorithm                    BitrotAlgorithm
	shouldFail, shouldFailQuorum bool
}{
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                             // 0
	{dataBlocks: 3, onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 1, algorithm: SHA256, shouldFail: false, shouldFailQuorum: false},                                 // 1
	{dataBlocks: 4, onDisks: 8, offDisks: 2, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 2, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                 // 2
	{dataBlocks: 5, onDisks: 10, offDisks: 3, blocksize: int64(blockSizeV2), data: oneMiByte, offset: oneMiByte, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                    // 3
	{dataBlocks: 6, onDisks: 12, offDisks: 4, blocksize: int64(blockSizeV2), data: oneMiByte, offset: oneMiByte, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                    // 4
	{dataBlocks: 7, onDisks: 14, offDisks: 5, blocksize: int64(blockSizeV2), data: 0, offset: 0, shouldFail: false, algorithm: SHA256, shouldFailQuorum: false},                                        // 5
	{dataBlocks: 8, onDisks: 16, offDisks: 7, blocksize: int64(blockSizeV2), data: 0, offset: 0, shouldFail: false, algorithm: DefaultBitrotAlgorithm, shouldFailQuorum: false},                        // 6
	{dataBlocks: 2, onDisks: 4, offDisks: 2, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: true},                              // 7
	{dataBlocks: 4, onDisks: 8, offDisks: 4, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: SHA256, shouldFail: false, shouldFailQuorum: true},                                  // 8
	{dataBlocks: 7, onDisks: 14, offDisks: 7, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                 // 9
	{dataBlocks: 8, onDisks: 16, offDisks: 8, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                 // 10
	{dataBlocks: 5, onDisks: 10, offDisks: 3, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                  // 11
	{dataBlocks: 3, onDisks: 6, offDisks: 1, blocksize: int64(blockSizeV2), data: oneMiByte, offset: oneMiByte / 2, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},     // 12
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(oneMiByte / 2), data: oneMiByte, offset: oneMiByte/2 + 1, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false}, // 13
	{dataBlocks: 4, onDisks: 8, offDisks: 0, blocksize: int64(oneMiByte - 1), data: oneMiByte, offset: oneMiByte - 1, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},               // 14
	{dataBlocks: 8, onDisks: 12, offDisks: 2, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 2, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                // 15
	{dataBlocks: 8, onDisks: 10, offDisks: 1, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                // 16
	{dataBlocks: 10, onDisks: 14, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 17, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},              // 17
	{dataBlocks: 2, onDisks: 6, offDisks: 2, blocksize: int64(oneMiByte), data: oneMiByte, offset: oneMiByte / 2, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},       // 18
	{dataBlocks: 10, onDisks: 16, offDisks: 8, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                // 19
}

func TestErasureEncode(t *testing.T) {
	for i, test := range erasureEncodeTests {
		setup, err := newErasureTestSetup(t, test.dataBlocks, test.onDisks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to create test setup: %v", i, err)
		}
		disks := setup.disks
		erasure, err := NewErasure(t.Context(), test.dataBlocks, test.onDisks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to create ErasureStorage: %v", i, err)
		}
		buffer := make([]byte, test.blocksize, 2*test.blocksize)

		data := make([]byte, test.data)
		if _, err = io.ReadFull(rand.Reader, data); err != nil {
			t.Fatalf("Test %d: failed to generate random test data: %v", i, err)
		}
		writers := make([]io.Writer, len(disks))
		for i, disk := range disks {
			if disk == OfflineDisk {
				continue
			}
			writers[i] = newBitrotWriter(disk, "", "testbucket", "object", erasure.ShardFileSize(int64(len(data[test.offset:]))), test.algorithm, erasure.ShardSize())
		}
		n, err := erasure.Encode(t.Context(), bytes.NewReader(data[test.offset:]), writers, buffer, erasure.dataBlocks+1)
		closeBitrotWriters(writers)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: should pass but failed with: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but it passed", i)
		}
		for i, w := range writers {
			if w == nil {
				disks[i] = OfflineDisk
			}
		}
		if err == nil {
			if length := int64(len(data[test.offset:])); n != length {
				t.Errorf("Test %d: invalid number of bytes written: got: #%d want #%d", i, n, length)
			}
			writers := make([]io.Writer, len(disks))
			for i, disk := range disks {
				if disk == nil {
					continue
				}
				writers[i] = newBitrotWriter(disk, "", "testbucket", "object2", erasure.ShardFileSize(int64(len(data[test.offset:]))), test.algorithm, erasure.ShardSize())
			}
			for j := range disks[:test.offDisks] {
				switch w := writers[j].(type) {
				case *wholeBitrotWriter:
					w.disk = badDisk{nil}
				case *streamingBitrotWriter:
					w.closeWithErr(errFaultyDisk)
				}
			}
			if test.offDisks > 0 {
				writers[0] = nil
			}
			n, err = erasure.Encode(t.Context(), bytes.NewReader(data[test.offset:]), writers, buffer, erasure.dataBlocks+1)
			closeBitrotWriters(writers)
			if err != nil && !test.shouldFailQuorum {
				t.Errorf("Test %d: should pass but failed with: %v", i, err)
			}
			if err == nil && test.shouldFailQuorum {
				t.Errorf("Test %d: should fail but it passed", i)
			}
			if err == nil {
				if length := int64(len(data[test.offset:])); n != length {
					t.Errorf("Test %d: invalid number of bytes written: got: #%d want #%d", i, n, length)
				}
			}
		}
	}
}

// Benchmarks

func benchmarkErasureEncode(data, parity, dataDown, parityDown int, size int64, b *testing.B) {
	setup, err := newErasureTestSetup(b, data, parity, blockSizeV2)
	if err != nil {
		b.Fatalf("failed to create test setup: %v", err)
	}
	erasure, err := NewErasure(context.Background(), data, parity, blockSizeV2)
	if err != nil {
		b.Fatalf("failed to create ErasureStorage: %v", err)
	}
	disks := setup.disks
	buffer := make([]byte, blockSizeV2, 2*blockSizeV2)
	content := make([]byte, size)

	for i := range dataDown {
		disks[i] = OfflineDisk
	}
	for i := data; i < data+parityDown; i++ {
		disks[i] = OfflineDisk
	}

	b.SetBytes(size)
	b.ReportAllocs()
	for b.Loop() {
		writers := make([]io.Writer, len(disks))
		for i, disk := range disks {
			if disk == OfflineDisk {
				continue
			}
			disk.Delete(context.Background(), "testbucket", "object", DeleteOptions{
				Recursive: false,
				Immediate: false,
			})
			writers[i] = newBitrotWriter(disk, "", "testbucket", "object", erasure.ShardFileSize(size), DefaultBitrotAlgorithm, erasure.ShardSize())
		}
		_, err := erasure.Encode(context.Background(), bytes.NewReader(content), writers, buffer, erasure.dataBlocks+1)
		closeBitrotWriters(writers)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkErasureEncodeQuick(b *testing.B) {
	const size = 12 * 1024 * 1024
	b.Run(" 00|00 ", func(b *testing.B) { benchmarkErasureEncode(2, 2, 0, 0, size, b) })
	b.Run(" 00|X0 ", func(b *testing.B) { benchmarkErasureEncode(2, 2, 0, 1, size, b) })
	b.Run(" X0|00 ", func(b *testing.B) { benchmarkErasureEncode(2, 2, 1, 0, size, b) })
}

func BenchmarkErasureEncode_4_64KB(b *testing.B) {
	const size = 64 * 1024
	b.Run(" 00|00 ", func(b *testing.B) { benchmarkErasureEncode(2, 2, 0, 0, size, b) })
	b.Run(" 00|X0 ", func(b *testing.B) { benchmarkErasureEncode(2, 2, 0, 1, size, b) })
	b.Run(" X0|00 ", func(b *testing.B) { benchmarkErasureEncode(2, 2, 1, 0, size, b) })
}

func BenchmarkErasureEncode_8_20MB(b *testing.B) {
	const size = 20 * 1024 * 1024
	b.Run(" 0000|0000 ", func(b *testing.B) { benchmarkErasureEncode(4, 4, 0, 0, size, b) })
	b.Run(" 0000|X000 ", func(b *testing.B) { benchmarkErasureEncode(4, 4, 0, 1, size, b) })
	b.Run(" X000|0000 ", func(b *testing.B) { benchmarkErasureEncode(4, 4, 1, 0, size, b) })
	b.Run(" 0000|XXX0 ", func(b *testing.B) { benchmarkErasureEncode(4, 4, 0, 3, size, b) })
	b.Run(" XXX0|0000 ", func(b *testing.B) { benchmarkErasureEncode(4, 4, 3, 0, size, b) })
}

func BenchmarkErasureEncode_12_30MB(b *testing.B) {
	const size = 30 * 1024 * 1024
	b.Run(" 000000|000000 ", func(b *testing.B) { benchmarkErasureEncode(6, 6, 0, 0, size, b) })
	b.Run(" 000000|X00000 ", func(b *testing.B) { benchmarkErasureEncode(6, 6, 0, 1, size, b) })
	b.Run(" X00000|000000 ", func(b *testing.B) { benchmarkErasureEncode(6, 6, 1, 0, size, b) })
	b.Run(" 000000|XXXXX0 ", func(b *testing.B) { benchmarkErasureEncode(6, 6, 0, 5, size, b) })
	b.Run(" XXXXX0|000000 ", func(b *testing.B) { benchmarkErasureEncode(6, 6, 5, 0, size, b) })
}

func BenchmarkErasureEncode_16_40MB(b *testing.B) {
	const size = 40 * 1024 * 1024
	b.Run(" 00000000|00000000 ", func(b *testing.B) { benchmarkErasureEncode(8, 8, 0, 0, size, b) })
	b.Run(" 00000000|X0000000 ", func(b *testing.B) { benchmarkErasureEncode(8, 8, 0, 1, size, b) })
	b.Run(" X0000000|00000000 ", func(b *testing.B) { benchmarkErasureEncode(8, 8, 1, 0, size, b) })
	b.Run(" 00000000|XXXXXXX0 ", func(b *testing.B) { benchmarkErasureEncode(8, 8, 0, 7, size, b) })
	b.Run(" XXXXXXX0|00000000 ", func(b *testing.B) { benchmarkErasureEncode(8, 8, 7, 0, size, b) })
}
