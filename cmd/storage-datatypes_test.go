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
	"encoding/gob"
	"io"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
)

func BenchmarkDecodeVolInfoMsgp(b *testing.B) {
	v := VolInfo{
		Name:    "uuid",
		Created: time.Now(),
	}
	var buf bytes.Buffer
	msgp.Encode(&buf, &v)
	rd := msgp.NewEndlessReader(buf.Bytes(), b)
	dc := msgp.NewReader(rd)
	b.Log("Size:", buf.Len(), "bytes")
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := v.DecodeMsg(dc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeDiskInfoMsgp(b *testing.B) {
	v := DiskInfo{
		Total:     1000,
		Free:      1000,
		Used:      1000,
		FSType:    "xfs",
		RootDisk:  true,
		Healing:   true,
		Endpoint:  "http://localhost:9001/tmp/drive1",
		MountPath: "/tmp/drive1",
		ID:        "uuid",
		Error:     "",
	}
	var buf bytes.Buffer
	msgp.Encode(&buf, &v)
	rd := msgp.NewEndlessReader(buf.Bytes(), b)
	dc := msgp.NewReader(rd)
	b.Log("Size:", buf.Len(), "bytes")
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := v.DecodeMsg(dc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeDiskInfoGOB(b *testing.B) {
	v := DiskInfo{
		Total:     1000,
		Free:      1000,
		Used:      1000,
		FSType:    "xfs",
		RootDisk:  true,
		Healing:   true,
		Endpoint:  "http://localhost:9001/tmp/drive1",
		MountPath: "/tmp/drive1",
		ID:        "uuid",
		Error:     "",
	}

	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(v)
	encoded := buf.Bytes()
	b.Log("Size:", buf.Len(), "bytes")
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		dec := gob.NewDecoder(bytes.NewBuffer(encoded))
		err := dec.Decode(&v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeDiskInfoMsgp(b *testing.B) {
	v := DiskInfo{
		Total:     1000,
		Free:      1000,
		Used:      1000,
		FSType:    "xfs",
		RootDisk:  true,
		Healing:   true,
		Endpoint:  "http://localhost:9001/tmp/drive1",
		MountPath: "/tmp/drive1",
		ID:        "uuid",
		Error:     "",
	}

	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := msgp.Encode(io.Discard, &v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeDiskInfoGOB(b *testing.B) {
	v := DiskInfo{
		Total:     1000,
		Free:      1000,
		Used:      1000,
		FSType:    "xfs",
		RootDisk:  true,
		Healing:   true,
		Endpoint:  "http://localhost:9001/tmp/drive1",
		MountPath: "/tmp/drive1",
		ID:        "uuid",
		Error:     "",
	}

	enc := gob.NewEncoder(io.Discard)
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := enc.Encode(&v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFileInfoMsgp(b *testing.B) {
	v := FileInfo{Volume: "testbucket", Name: "src/compress/zlib/reader_test.go", VersionID: "", IsLatest: true, Deleted: false, DataDir: "5e0153cc-621a-4267-8cb6-4919140d53b3", XLV1: false, ModTime: UTCNow(), Size: 3430, Mode: 0x0, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption-Iv": "jIJPsrkkVYYMvc7edBrNl+7zcM7+ZwXqMb/YAjBO/ck=", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "my-minio-key", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAP2p7ZLv3UpLwBnsKkF2mtWba0qoY42tymK0szRgGvAxBNcXyHXYooe9dQpeeEJWgKUa/8R61oCy1mFwIg==", "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "IAAfAPFYRDkHVirJBJxBixNj3PLWt78dFuUTyTLIdLG820J7XqLPBO4gpEEEWw/DoTsJIb+apnaem+rKtQ1h3Q==", "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256", "content-type": "application/octet-stream", "etag": "20000f00e2c3709dc94905c6ce31e1cadbd1c064e14acdcd44cf0ac2db777eeedd88d639fcd64de16851ade8b21a9a1a"}, Parts: []ObjectPartInfo{{ETag: "", Number: 1, Size: 3430, ActualSize: 3398}}, Erasure: ErasureInfo{Algorithm: "reedsolomon", DataBlocks: 2, ParityBlocks: 2, BlockSize: 10485760, Index: 3, Distribution: []int{3, 4, 1, 2}, Checksums: []ChecksumInfo{{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}}}}}
	var buf bytes.Buffer
	msgp.Encode(&buf, &v)
	rd := msgp.NewEndlessReader(buf.Bytes(), b)
	dc := msgp.NewReader(rd)
	b.Log("Size:", buf.Len(), "bytes")
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := v.DecodeMsg(dc)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFileInfoGOB(b *testing.B) {
	v := FileInfo{Volume: "testbucket", Name: "src/compress/zlib/reader_test.go", VersionID: "", IsLatest: true, Deleted: false, DataDir: "5e0153cc-621a-4267-8cb6-4919140d53b3", XLV1: false, ModTime: UTCNow(), Size: 3430, Mode: 0x0, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption-Iv": "jIJPsrkkVYYMvc7edBrNl+7zcM7+ZwXqMb/YAjBO/ck=", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "my-minio-key", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAP2p7ZLv3UpLwBnsKkF2mtWba0qoY42tymK0szRgGvAxBNcXyHXYooe9dQpeeEJWgKUa/8R61oCy1mFwIg==", "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "IAAfAPFYRDkHVirJBJxBixNj3PLWt78dFuUTyTLIdLG820J7XqLPBO4gpEEEWw/DoTsJIb+apnaem+rKtQ1h3Q==", "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256", "content-type": "application/octet-stream", "etag": "20000f00e2c3709dc94905c6ce31e1cadbd1c064e14acdcd44cf0ac2db777eeedd88d639fcd64de16851ade8b21a9a1a"}, Parts: []ObjectPartInfo{{ETag: "", Number: 1, Size: 3430, ActualSize: 3398}}, Erasure: ErasureInfo{Algorithm: "reedsolomon", DataBlocks: 2, ParityBlocks: 2, BlockSize: 10485760, Index: 3, Distribution: []int{3, 4, 1, 2}, Checksums: []ChecksumInfo{{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}}}}}
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(v)
	encoded := buf.Bytes()
	b.Log("Size:", buf.Len(), "bytes")
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		dec := gob.NewDecoder(bytes.NewBuffer(encoded))
		err := dec.Decode(&v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFileInfoMsgp(b *testing.B) {
	v := FileInfo{Volume: "testbucket", Name: "src/compress/zlib/reader_test.go", VersionID: "", IsLatest: true, Deleted: false, DataDir: "5e0153cc-621a-4267-8cb6-4919140d53b3", XLV1: false, ModTime: UTCNow(), Size: 3430, Mode: 0x0, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption-Iv": "jIJPsrkkVYYMvc7edBrNl+7zcM7+ZwXqMb/YAjBO/ck=", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "my-minio-key", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAP2p7ZLv3UpLwBnsKkF2mtWba0qoY42tymK0szRgGvAxBNcXyHXYooe9dQpeeEJWgKUa/8R61oCy1mFwIg==", "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "IAAfAPFYRDkHVirJBJxBixNj3PLWt78dFuUTyTLIdLG820J7XqLPBO4gpEEEWw/DoTsJIb+apnaem+rKtQ1h3Q==", "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256", "content-type": "application/octet-stream", "etag": "20000f00e2c3709dc94905c6ce31e1cadbd1c064e14acdcd44cf0ac2db777eeedd88d639fcd64de16851ade8b21a9a1a"}, Parts: []ObjectPartInfo{{ETag: "", Number: 1, Size: 3430, ActualSize: 3398}}, Erasure: ErasureInfo{Algorithm: "reedsolomon", DataBlocks: 2, ParityBlocks: 2, BlockSize: 10485760, Index: 3, Distribution: []int{3, 4, 1, 2}, Checksums: []ChecksumInfo{{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}}}}}
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := msgp.Encode(io.Discard, &v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFileInfoGOB(b *testing.B) {
	v := FileInfo{Volume: "testbucket", Name: "src/compress/zlib/reader_test.go", VersionID: "", IsLatest: true, Deleted: false, DataDir: "5e0153cc-621a-4267-8cb6-4919140d53b3", XLV1: false, ModTime: UTCNow(), Size: 3430, Mode: 0x0, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption-Iv": "jIJPsrkkVYYMvc7edBrNl+7zcM7+ZwXqMb/YAjBO/ck=", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "my-minio-key", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAP2p7ZLv3UpLwBnsKkF2mtWba0qoY42tymK0szRgGvAxBNcXyHXYooe9dQpeeEJWgKUa/8R61oCy1mFwIg==", "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "IAAfAPFYRDkHVirJBJxBixNj3PLWt78dFuUTyTLIdLG820J7XqLPBO4gpEEEWw/DoTsJIb+apnaem+rKtQ1h3Q==", "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256", "content-type": "application/octet-stream", "etag": "20000f00e2c3709dc94905c6ce31e1cadbd1c064e14acdcd44cf0ac2db777eeedd88d639fcd64de16851ade8b21a9a1a"}, Parts: []ObjectPartInfo{{ETag: "", Number: 1, Size: 3430, ActualSize: 3398}}, Erasure: ErasureInfo{Algorithm: "reedsolomon", DataBlocks: 2, ParityBlocks: 2, BlockSize: 10485760, Index: 3, Distribution: []int{3, 4, 1, 2}, Checksums: []ChecksumInfo{{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}}}}}
	enc := gob.NewEncoder(io.Discard)
	b.SetBytes(1)
	b.ReportAllocs()

	for b.Loop() {
		err := enc.Encode(&v)
		if err != nil {
			b.Fatal(err)
		}
	}
}
