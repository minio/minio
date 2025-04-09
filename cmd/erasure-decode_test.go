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
	crand "crypto/rand"
	"io"
	"math/rand"
	"testing"

	"github.com/dustin/go-humanize"
)

func (a badDisk) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	return 0, errFaultyDisk
}

var erasureDecodeTests = []struct {
	dataBlocks                   int
	onDisks, offDisks            int
	blocksize, data              int64
	offset                       int64
	length                       int64
	algorithm                    BitrotAlgorithm
	shouldFail, shouldFailQuorum bool
}{
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},             // 0
	{dataBlocks: 3, onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: SHA256, shouldFail: false, shouldFailQuorum: false},                 // 1
	{dataBlocks: 4, onDisks: 8, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false}, // 2
	{dataBlocks: 5, onDisks: 10, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 1, length: oneMiByte - 1, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},        // 3
	{dataBlocks: 6, onDisks: 12, offDisks: 0, blocksize: int64(oneMiByte), data: oneMiByte, offset: oneMiByte, length: 0, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},
	// 4
	{dataBlocks: 7, onDisks: 14, offDisks: 0, blocksize: int64(oneMiByte), data: oneMiByte, offset: 3, length: 1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                    // 5
	{dataBlocks: 8, onDisks: 16, offDisks: 0, blocksize: int64(oneMiByte), data: oneMiByte, offset: 4, length: 8 * 1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                // 6
	{dataBlocks: 7, onDisks: 14, offDisks: 7, blocksize: int64(blockSizeV2), data: oneMiByte, offset: oneMiByte, length: 1, algorithm: DefaultBitrotAlgorithm, shouldFail: true, shouldFailQuorum: false},              // 7
	{dataBlocks: 6, onDisks: 12, offDisks: 6, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},             // 8
	{dataBlocks: 5, onDisks: 10, offDisks: 5, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                           // 9
	{dataBlocks: 4, onDisks: 8, offDisks: 4, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: SHA256, shouldFail: false, shouldFailQuorum: false},                              // 10
	{dataBlocks: 3, onDisks: 6, offDisks: 3, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                // 11
	{dataBlocks: 2, onDisks: 4, offDisks: 2, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},              // 12
	{dataBlocks: 2, onDisks: 4, offDisks: 1, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                // 13
	{dataBlocks: 3, onDisks: 6, offDisks: 2, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                // 14
	{dataBlocks: 4, onDisks: 8, offDisks: 3, blocksize: int64(2 * oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},            // 15
	{dataBlocks: 5, onDisks: 10, offDisks: 6, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                // 16
	{dataBlocks: 5, onDisks: 10, offDisks: 2, blocksize: int64(blockSizeV2), data: 2 * oneMiByte, offset: oneMiByte, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false}, // 17
	{dataBlocks: 5, onDisks: 10, offDisks: 1, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                         // 18
	{dataBlocks: 6, onDisks: 12, offDisks: 3, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: SHA256, shouldFail: false, shouldFailQuorum: false},
	// 19
	{dataBlocks: 6, onDisks: 12, offDisks: 7, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                                             // 20
	{dataBlocks: 8, onDisks: 16, offDisks: 8, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                            // 21
	{dataBlocks: 8, onDisks: 16, offDisks: 9, blocksize: int64(oneMiByte), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},                                               // 22
	{dataBlocks: 8, onDisks: 16, offDisks: 7, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                            // 23
	{dataBlocks: 2, onDisks: 4, offDisks: 1, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                             // 24
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: oneMiByte, offset: 0, length: oneMiByte, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},                                             // 25
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(blockSizeV2) + 1, offset: 0, length: int64(blockSizeV2) + 1, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                               // 26
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 12, length: int64(blockSizeV2) + 17, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                             // 27
	{dataBlocks: 3, onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 1023, length: int64(blockSizeV2) + 1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},             // 28
	{dataBlocks: 4, onDisks: 8, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 11, length: int64(blockSizeV2) + 2*1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},             // 29
	{dataBlocks: 6, onDisks: 12, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 512, length: int64(blockSizeV2) + 8*1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},           // 30
	{dataBlocks: 8, onDisks: 16, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: int64(blockSizeV2), length: int64(blockSizeV2) - 1, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false}, // 31
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(oneMiByte), offset: -1, length: 3, algorithm: DefaultBitrotAlgorithm, shouldFail: true, shouldFailQuorum: false},                                              // 32
	{dataBlocks: 2, onDisks: 4, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(oneMiByte), offset: 1024, length: -1, algorithm: DefaultBitrotAlgorithm, shouldFail: true, shouldFailQuorum: false},                                           // 33
	{dataBlocks: 4, onDisks: 6, offDisks: 0, blocksize: int64(blockSizeV2), data: int64(blockSizeV2), offset: 0, length: int64(blockSizeV2), algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                                       // 34
	{dataBlocks: 4, onDisks: 6, offDisks: 1, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 12, length: int64(blockSizeV2) + 17, algorithm: BLAKE2b512, shouldFail: false, shouldFailQuorum: false},                             // 35
	{dataBlocks: 4, onDisks: 6, offDisks: 3, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 1023, length: int64(blockSizeV2) + 1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: true},              // 36
	{dataBlocks: 8, onDisks: 12, offDisks: 4, blocksize: int64(blockSizeV2), data: int64(2 * blockSizeV2), offset: 11, length: int64(blockSizeV2) + 2*1024, algorithm: DefaultBitrotAlgorithm, shouldFail: false, shouldFailQuorum: false},            // 37
}

func TestErasureDecode(t *testing.T) {
	for i, test := range erasureDecodeTests {
		setup, err := newErasureTestSetup(t, test.dataBlocks, test.onDisks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to create test setup: %v", i, err)
		}
		erasure, err := NewErasure(t.Context(), test.dataBlocks, test.onDisks-test.dataBlocks, test.blocksize)
		if err != nil {
			t.Fatalf("Test %d: failed to create ErasureStorage: %v", i, err)
		}
		disks := setup.disks
		data := make([]byte, test.data)
		if _, err = io.ReadFull(crand.Reader, data); err != nil {
			t.Fatalf("Test %d: failed to generate random test data: %v", i, err)
		}

		writeAlgorithm := test.algorithm
		if !test.algorithm.Available() {
			writeAlgorithm = DefaultBitrotAlgorithm
		}
		buffer := make([]byte, test.blocksize, 2*test.blocksize)
		writers := make([]io.Writer, len(disks))
		for i, disk := range disks {
			writers[i] = newBitrotWriter(disk, "", "testbucket", "object", erasure.ShardFileSize(test.data), writeAlgorithm, erasure.ShardSize())
		}
		n, err := erasure.Encode(t.Context(), bytes.NewReader(data), writers, buffer, erasure.dataBlocks+1)
		closeBitrotWriters(writers)
		if err != nil {
			t.Fatalf("Test %d: failed to create erasure test file: %v", i, err)
		}
		if n != test.data {
			t.Fatalf("Test %d: failed to create erasure test file", i)
		}
		for i, w := range writers {
			if w == nil {
				disks[i] = nil
			}
		}

		// Get the checksums of the current part.
		bitrotReaders := make([]io.ReaderAt, len(disks))
		for index, disk := range disks {
			if disk == OfflineDisk {
				continue
			}
			tillOffset := erasure.ShardFileOffset(test.offset, test.length, test.data)

			bitrotReaders[index] = newBitrotReader(disk, nil, "testbucket", "object", tillOffset, writeAlgorithm, bitrotWriterSum(writers[index]), erasure.ShardSize())
		}

		writer := bytes.NewBuffer(nil)
		_, err = erasure.Decode(t.Context(), writer, bitrotReaders, test.offset, test.length, test.data, nil)
		closeBitrotReaders(bitrotReaders)
		if err != nil && !test.shouldFail {
			t.Errorf("Test %d: should pass but failed with: %v", i, err)
		}
		if err == nil && test.shouldFail {
			t.Errorf("Test %d: should fail but it passed", i)
		}
		if err == nil {
			if content := writer.Bytes(); !bytes.Equal(content, data[test.offset:test.offset+test.length]) {
				t.Errorf("Test %d: read returns wrong file content.", i)
			}
		}

		for i, r := range bitrotReaders {
			if r == nil {
				disks[i] = OfflineDisk
			}
		}
		if err == nil && !test.shouldFail {
			bitrotReaders = make([]io.ReaderAt, len(disks))
			for index, disk := range disks {
				if disk == OfflineDisk {
					continue
				}
				tillOffset := erasure.ShardFileOffset(test.offset, test.length, test.data)
				bitrotReaders[index] = newBitrotReader(disk, nil, "testbucket", "object", tillOffset, writeAlgorithm, bitrotWriterSum(writers[index]), erasure.ShardSize())
			}
			for j := range disks[:test.offDisks] {
				if bitrotReaders[j] == nil {
					continue
				}
				switch r := bitrotReaders[j].(type) {
				case *wholeBitrotReader:
					r.disk = badDisk{nil}
				case *streamingBitrotReader:
					r.disk = badDisk{nil}
				}
			}
			if test.offDisks > 0 {
				bitrotReaders[0] = nil
			}
			writer.Reset()
			_, err = erasure.Decode(t.Context(), writer, bitrotReaders, test.offset, test.length, test.data, nil)
			closeBitrotReaders(bitrotReaders)
			if err != nil && !test.shouldFailQuorum {
				t.Errorf("Test %d: should pass but failed with: %v", i, err)
			}
			if err == nil && test.shouldFailQuorum {
				t.Errorf("Test %d: should fail but it passed", i)
			}
			if !test.shouldFailQuorum {
				if content := writer.Bytes(); !bytes.Equal(content, data[test.offset:test.offset+test.length]) {
					t.Errorf("Test %d: read returns wrong file content", i)
				}
			}
		}
	}
}

// Test erasureDecode with random offset and lengths.
// This test is t.Skip()ed as it a long time to run, hence should be run
// explicitly after commenting out t.Skip()
func TestErasureDecodeRandomOffsetLength(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Initialize environment needed for the test.
	dataBlocks := 7
	parityBlocks := 7
	blockSize := int64(1 * humanize.MiByte)
	setup, err := newErasureTestSetup(t, dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Error(err)
		return
	}
	disks := setup.disks
	erasure, err := NewErasure(t.Context(), dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Fatalf("failed to create ErasureStorage: %v", err)
	}
	// Prepare a slice of 5MiB with random data.
	data := make([]byte, 5*humanize.MiByte)
	length := int64(len(data))
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	writers := make([]io.Writer, len(disks))
	for i, disk := range disks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, "", "testbucket", "object", erasure.ShardFileSize(length), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	// 10000 iterations with random offsets and lengths.
	iterations := 10000

	// Create a test file to read from.
	buffer := make([]byte, blockSize, 2*blockSize)
	n, err := erasure.Encode(t.Context(), bytes.NewReader(data), writers, buffer, erasure.dataBlocks+1)
	closeBitrotWriters(writers)
	if err != nil {
		t.Fatal(err)
	}
	if n != length {
		t.Errorf("erasureCreateFile returned %d, expected %d", n, length)
	}

	// To generate random offset/length.
	r := rand.New(rand.NewSource(UTCNow().UnixNano()))

	buf := &bytes.Buffer{}

	// Verify erasure.Decode() for random offsets and lengths.
	for i := 0; i < iterations; i++ {
		offset := r.Int63n(length)
		readLen := r.Int63n(length - offset)

		expected := data[offset : offset+readLen]

		// Get the checksums of the current part.
		bitrotReaders := make([]io.ReaderAt, len(disks))
		for index, disk := range disks {
			if disk == OfflineDisk {
				continue
			}
			tillOffset := erasure.ShardFileOffset(offset, readLen, length)
			bitrotReaders[index] = newStreamingBitrotReader(disk, nil, "testbucket", "object", tillOffset, DefaultBitrotAlgorithm, erasure.ShardSize())
		}
		_, err = erasure.Decode(t.Context(), buf, bitrotReaders, offset, readLen, length, nil)
		closeBitrotReaders(bitrotReaders)
		if err != nil {
			t.Fatal(err, offset, readLen)
		}
		got := buf.Bytes()
		if !bytes.Equal(expected, got) {
			t.Fatalf("read data is different from what was expected, offset=%d length=%d", offset, readLen)
		}
		buf.Reset()
	}
}

// Benchmarks

func benchmarkErasureDecode(data, parity, dataDown, parityDown int, size int64, b *testing.B) {
	setup, err := newErasureTestSetup(b, data, parity, blockSizeV2)
	if err != nil {
		b.Fatalf("failed to create test setup: %v", err)
	}
	disks := setup.disks
	erasure, err := NewErasure(context.Background(), data, parity, blockSizeV2)
	if err != nil {
		b.Fatalf("failed to create ErasureStorage: %v", err)
	}

	writers := make([]io.Writer, len(disks))
	for i, disk := range disks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, "", "testbucket", "object", erasure.ShardFileSize(size), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	content := make([]byte, size)
	buffer := make([]byte, blockSizeV2, 2*blockSizeV2)
	_, err = erasure.Encode(context.Background(), bytes.NewReader(content), writers, buffer, erasure.dataBlocks+1)
	closeBitrotWriters(writers)
	if err != nil {
		b.Fatalf("failed to create erasure test file: %v", err)
	}

	for i := 0; i < dataDown; i++ {
		writers[i] = nil
	}
	for i := data; i < data+parityDown; i++ {
		writers[i] = nil
	}

	b.ResetTimer()
	b.SetBytes(size)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bitrotReaders := make([]io.ReaderAt, len(disks))
		for index, disk := range disks {
			if writers[index] == nil {
				continue
			}
			tillOffset := erasure.ShardFileOffset(0, size, size)
			bitrotReaders[index] = newStreamingBitrotReader(disk, nil, "testbucket", "object", tillOffset, DefaultBitrotAlgorithm, erasure.ShardSize())
		}
		if _, err = erasure.Decode(context.Background(), bytes.NewBuffer(content[:0]), bitrotReaders, 0, size, size, nil); err != nil {
			panic(err)
		}
		closeBitrotReaders(bitrotReaders)
	}
}

func BenchmarkErasureDecodeQuick(b *testing.B) {
	const size = 12 * 1024 * 1024
	b.Run(" 00|00 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 0, 0, size, b) })
	b.Run(" 00|X0 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 0, 1, size, b) })
	b.Run(" X0|00 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 1, 0, size, b) })
	b.Run(" X0|X0 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 1, 1, size, b) })
}

func BenchmarkErasureDecode_4_64KB(b *testing.B) {
	const size = 64 * 1024
	b.Run(" 00|00 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 0, 0, size, b) })
	b.Run(" 00|X0 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 0, 1, size, b) })
	b.Run(" X0|00 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 1, 0, size, b) })
	b.Run(" X0|X0 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 1, 1, size, b) })
	b.Run(" 00|XX ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 0, 2, size, b) })
	b.Run(" XX|00 ", func(b *testing.B) { benchmarkErasureDecode(2, 2, 2, 0, size, b) })
}

func BenchmarkErasureDecode_8_20MB(b *testing.B) {
	const size = 20 * 1024 * 1024
	b.Run(" 0000|0000 ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 0, 0, size, b) })
	b.Run(" 0000|X000 ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 0, 1, size, b) })
	b.Run(" X000|0000 ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 1, 0, size, b) })
	b.Run(" X000|X000 ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 1, 1, size, b) })
	b.Run(" 0000|XXXX ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 0, 4, size, b) })
	b.Run(" XX00|XX00 ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 2, 2, size, b) })
	b.Run(" XXXX|0000 ", func(b *testing.B) { benchmarkErasureDecode(4, 4, 4, 0, size, b) })
}

func BenchmarkErasureDecode_12_30MB(b *testing.B) {
	const size = 30 * 1024 * 1024
	b.Run(" 000000|000000 ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 0, 0, size, b) })
	b.Run(" 000000|X00000 ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 0, 1, size, b) })
	b.Run(" X00000|000000 ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 1, 0, size, b) })
	b.Run(" X00000|X00000 ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 1, 1, size, b) })
	b.Run(" 000000|XXXXXX ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 0, 6, size, b) })
	b.Run(" XXX000|XXX000 ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 3, 3, size, b) })
	b.Run(" XXXXXX|000000 ", func(b *testing.B) { benchmarkErasureDecode(6, 6, 6, 0, size, b) })
}

func BenchmarkErasureDecode_16_40MB(b *testing.B) {
	const size = 40 * 1024 * 1024
	b.Run(" 00000000|00000000 ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 0, 0, size, b) })
	b.Run(" 00000000|X0000000 ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 0, 1, size, b) })
	b.Run(" X0000000|00000000 ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 1, 0, size, b) })
	b.Run(" X0000000|X0000000 ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 1, 1, size, b) })
	b.Run(" 00000000|XXXXXXXX ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 0, 8, size, b) })
	b.Run(" XXXX0000|XXXX0000 ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 4, 4, size, b) })
	b.Run(" XXXXXXXX|00000000 ", func(b *testing.B) { benchmarkErasureDecode(8, 8, 8, 0, size, b) })
}
