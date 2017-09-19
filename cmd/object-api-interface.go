/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"

	sha256 "github.com/minio/sha256-simd"
)

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Storage operations.
	Shutdown() error
	StorageInfo() StorageInfo

	// Bucket operations.
	MakeBucketWithLocation(bucket string, location string) error
	GetBucketInfo(bucket string) (bucketInfo BucketInfo, err error)
	ListBuckets() (buckets []BucketInfo, err error)
	DeleteBucket(bucket string) error
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error)

	// Object operations.
	GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error)
	GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error)
	PutObject(bucket, object string, data *HashReader, metadata map[string]string) (objInfo ObjectInfo, err error)
	CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error)
	DeleteObject(bucket, object string) error

	// Multipart operations.
	ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error)
	NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error)
	CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error)
	PutObjectPart(bucket, object, uploadID string, partID int, data *HashReader) (info PartInfo, err error)
	ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error)
	AbortMultipartUpload(bucket, object, uploadID string) error
	CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error)

	// Healing operations.
	HealBucket(bucket string) error
	ListBucketsHeal() (buckets []BucketInfo, err error)
	HealObject(bucket, object string) (int, int, error)
	ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error)
	ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
		delimiter string, maxUploads int) (ListMultipartsInfo, error)
}

// HashReader writes what it reads from an io.Reader to an MD5 and SHA256 hash.Hash.
// HashReader verifies that the content of the io.Reader matches the expected checksums.
type HashReader struct {
	src                 io.Reader
	size                int64
	md5Hash, sha256Hash hash.Hash
	md5Sum, sha256Sum   string // hex representation
}

// NewHashReader returns a new HashReader computing the MD5 sum and SHA256 sum
// (if set) of the provided io.Reader.
func NewHashReader(src io.Reader, size int64, md5Sum, sha256Sum string) *HashReader {
	var sha256Hash hash.Hash
	if sha256Sum != "" {
		sha256Hash = sha256.New()
	}
	if size >= 0 {
		src = io.LimitReader(src, size)
	} else {
		size = -1
	}
	return &HashReader{
		src:        src,
		size:       size,
		md5Sum:     md5Sum,
		sha256Sum:  sha256Sum,
		md5Hash:    md5.New(),
		sha256Hash: sha256Hash,
	}
}

func (r *HashReader) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	if err != nil && err != io.EOF {
		return
	}
	if r.md5Hash != nil {
		r.md5Hash.Write(p[:n])
	}
	if r.sha256Hash != nil {
		r.sha256Hash.Write(p[:n])
	}
	return
}

// Size returns the absolute number of bytes the HashReader
// will return during reading. It returns -1 for unlimited
// data.
func (r *HashReader) Size() int64 { return r.size }

// MD5 returns the MD5 sum of the processed data. Any
// further reads will change the MD5 sum.
func (r *HashReader) MD5() []byte { return r.md5Hash.Sum(nil) }

// Verify verifies if the computed MD5 sum - and SHA256 sum - are
// equal to the ones specified when creating the HashReader.
func (r *HashReader) Verify() error {
	if r.sha256Hash != nil {
		sha256Sum, err := hex.DecodeString(r.sha256Sum)
		if err != nil {
			return SHA256Mismatch{}
		}
		if !bytes.Equal(sha256Sum, r.sha256Hash.Sum(nil)) {
			return errContentSHA256Mismatch
		}
	}
	if r.md5Hash != nil && r.md5Sum != "" {
		md5Sum, err := hex.DecodeString(r.md5Sum)
		if err != nil {
			return BadDigest{r.md5Sum, hex.EncodeToString(r.md5Hash.Sum(nil))}
		}
		if sum := r.md5Hash.Sum(nil); !bytes.Equal(md5Sum, sum) {
			return BadDigest{r.md5Sum, hex.EncodeToString(sum)}
		}
	}
	return nil
}
