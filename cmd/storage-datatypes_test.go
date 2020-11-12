/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

func BenchmarkDecodeDiskInfoMsgp(b *testing.B) {
	v := DiskInfo{
		Total:     1000,
		Free:      1000,
		Used:      1000,
		FSType:    "xfs",
		RootDisk:  true,
		Healing:   true,
		Endpoint:  "http://localhost:9001/tmp/disk1",
		MountPath: "/tmp/disk1",
		ID:        "uuid",
		Error:     "",
	}
	var buf bytes.Buffer
	msgp.Encode(&buf, &v)
	b.SetBytes(1)
	rd := msgp.NewEndlessReader(buf.Bytes(), b)
	dc := msgp.NewReader(rd)
	b.Log("Size:", buf.Len(), "bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
		Endpoint:  "http://localhost:9001/tmp/disk1",
		MountPath: "/tmp/disk1",
		ID:        "uuid",
		Error:     "",
	}

	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(v)
	encoded := buf.Bytes()
	b.Log("Size:", buf.Len(), "bytes")
	b.SetBytes(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
		Endpoint:  "http://localhost:9001/tmp/disk1",
		MountPath: "/tmp/disk1",
		ID:        "uuid",
		Error:     "",
	}

	b.SetBytes(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := msgp.Encode(ioutil.Discard, &v)
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
		Endpoint:  "http://localhost:9001/tmp/disk1",
		MountPath: "/tmp/disk1",
		ID:        "uuid",
		Error:     "",
	}

	enc := gob.NewEncoder(ioutil.Discard)
	b.SetBytes(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.SetBytes(1)
	rd := msgp.NewEndlessReader(buf.Bytes(), b)
	dc := msgp.NewReader(rd)
	b.Log("Size:", buf.Len(), "bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := msgp.Encode(ioutil.Discard, &v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFileInfoGOB(b *testing.B) {
	v := FileInfo{Volume: "testbucket", Name: "src/compress/zlib/reader_test.go", VersionID: "", IsLatest: true, Deleted: false, DataDir: "5e0153cc-621a-4267-8cb6-4919140d53b3", XLV1: false, ModTime: UTCNow(), Size: 3430, Mode: 0x0, Metadata: map[string]string{"X-Minio-Internal-Server-Side-Encryption-Iv": "jIJPsrkkVYYMvc7edBrNl+7zcM7+ZwXqMb/YAjBO/ck=", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "my-minio-key", "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAP2p7ZLv3UpLwBnsKkF2mtWba0qoY42tymK0szRgGvAxBNcXyHXYooe9dQpeeEJWgKUa/8R61oCy1mFwIg==", "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": "IAAfAPFYRDkHVirJBJxBixNj3PLWt78dFuUTyTLIdLG820J7XqLPBO4gpEEEWw/DoTsJIb+apnaem+rKtQ1h3Q==", "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256", "content-type": "application/octet-stream", "etag": "20000f00e2c3709dc94905c6ce31e1cadbd1c064e14acdcd44cf0ac2db777eeedd88d639fcd64de16851ade8b21a9a1a"}, Parts: []ObjectPartInfo{{ETag: "", Number: 1, Size: 3430, ActualSize: 3398}}, Erasure: ErasureInfo{Algorithm: "reedsolomon", DataBlocks: 2, ParityBlocks: 2, BlockSize: 10485760, Index: 3, Distribution: []int{3, 4, 1, 2}, Checksums: []ChecksumInfo{{PartNumber: 1, Algorithm: 0x3, Hash: []uint8{}}}}}
	enc := gob.NewEncoder(ioutil.Discard)
	b.SetBytes(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := enc.Encode(&v)
		if err != nil {
			b.Fatal(err)
		}
	}
}
