/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

// InvalidArgument invalid argument
type InvalidArgument struct{}

func (e InvalidArgument) Error() string {
	return "Invalid argument"
}

// UnsupportedFilesystem unsupported filesystem type
type UnsupportedFilesystem struct {
	Type string
}

func (e UnsupportedFilesystem) Error() string {
	return "Unsupported filesystem: " + e.Type
}

// BucketNotFound bucket does not exist
type BucketNotFound struct {
	Bucket string
}

func (e BucketNotFound) Error() string {
	return "Bucket not found: " + e.Bucket
}

// ObjectExists object exists
type ObjectExists struct {
	Object string
}

func (e ObjectExists) Error() string {
	return "Object exists: " + e.Object
}

// ObjectNotFound object does not exist
type ObjectNotFound struct {
	Object string
}

func (e ObjectNotFound) Error() string {
	return "Object not found: " + e.Object
}

// ObjectCorrupted object found to be corrupted
type ObjectCorrupted struct {
	Object string
}

func (e ObjectCorrupted) Error() string {
	return "Object found corrupted: " + e.Object
}

// BucketExists bucket exists
type BucketExists struct {
	Bucket string
}

func (e BucketExists) Error() string {
	return "Bucket exists: " + e.Bucket
}

// CorruptedBackend backend found to be corrupted
type CorruptedBackend struct {
	Backend string
}

func (e CorruptedBackend) Error() string {
	return "Corrupted backend: " + e.Backend
}

// NotImplemented function not implemented
type NotImplemented struct {
	Function string
}

func (e NotImplemented) Error() string {
	return "Not implemented: " + e.Function
}

// InvalidDisksArgument invalid number of disks per node
type InvalidDisksArgument struct{}

func (e InvalidDisksArgument) Error() string {
	return "Invalid number of disks per node"
}

// BadDigest bad md5sum
type BadDigest struct{}

func (e BadDigest) Error() string {
	return "Bad digest"
}

// ParityOverflow parity over flow
type ParityOverflow struct{}

func (e ParityOverflow) Error() string {
	return "Parity overflow"
}

// ChecksumMismatch checksum mismatch
type ChecksumMismatch struct{}

func (e ChecksumMismatch) Error() string {
	return "Checksum mismatch"
}

// MissingErasureTechnique missing erasure technique
type MissingErasureTechnique struct{}

func (e MissingErasureTechnique) Error() string {
	return "Missing erasure technique"
}

// InvalidErasureTechnique invalid erasure technique
type InvalidErasureTechnique struct {
	Technique string
}

func (e InvalidErasureTechnique) Error() string {
	return "Invalid erasure technique: " + e.Technique
}
