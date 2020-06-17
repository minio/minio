/*
 * MinIO Cloud Storage, (C) 2017, 2018 MinIO, Inc.
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
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v6/pkg/encrypt"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

var (
	// AWS errors for invalid SSE-C requests.
	errEncryptedObject      = errors.New("The object was stored using a form of SSE")
	errInvalidSSEParameters = errors.New("The SSE-C key for key-rotation is not correct") // special access denied
	errKMSNotConfigured     = errors.New("KMS not configured for a server side encrypted object")
	// Additional MinIO errors for SSE-C requests.
	errObjectTampered = errors.New("The requested object was modified and may be compromised")
	// error returned when invalid encryption parameters are specified
	errInvalidEncryptionParameters = errors.New("The encryption parameters are not applicable to this object")
)

const (
	// SSECustomerKeySize is the size of valid client provided encryption keys in bytes.
	// Currently AWS supports only AES256. So the SSE-C key size is fixed to 32 bytes.
	SSECustomerKeySize = 32

	// SSEIVSize is the size of the IV data
	SSEIVSize = 32 // 32 bytes

	// SSEDAREPackageBlockSize - SSE dare package block size.
	SSEDAREPackageBlockSize = 64 * 1024 // 64KiB bytes

	// SSEDAREPackageMetaSize - SSE dare package meta padding bytes.
	SSEDAREPackageMetaSize = 32 // 32 bytes

)

// isEncryptedMultipart returns true if the current object is
// uploaded by the user using multipart mechanism:
// initiate new multipart, upload part, complete upload
func isEncryptedMultipart(objInfo ObjectInfo) bool {
	if len(objInfo.Parts) == 0 {
		return false
	}
	if !crypto.IsMultiPart(objInfo.UserDefined) {
		return false
	}
	for _, part := range objInfo.Parts {
		_, err := sio.DecryptedSize(uint64(part.Size))
		if err != nil {
			return false
		}
	}
	// Further check if this object is uploaded using multipart mechanism
	// by the user and it is not about Erasure internally splitting the
	// object into parts in PutObject()
	return !(objInfo.backendType == BackendErasure && len(objInfo.ETag) == 32)
}

// ParseSSECopyCustomerRequest parses the SSE-C header fields of the provided request.
// It returns the client provided key on success.
func ParseSSECopyCustomerRequest(h http.Header, metadata map[string]string) (key []byte, err error) {
	if crypto.S3.IsEncrypted(metadata) && crypto.SSECopy.IsRequested(h) {
		return nil, crypto.ErrIncompatibleEncryptionMethod
	}
	k, err := crypto.SSECopy.ParseHTTP(h)
	return k[:], err
}

// ParseSSECustomerRequest parses the SSE-C header fields of the provided request.
// It returns the client provided key on success.
func ParseSSECustomerRequest(r *http.Request) (key []byte, err error) {
	return ParseSSECustomerHeader(r.Header)
}

// ParseSSECustomerHeader parses the SSE-C header fields and returns
// the client provided key on success.
func ParseSSECustomerHeader(header http.Header) (key []byte, err error) {
	if crypto.S3.IsRequested(header) && crypto.SSEC.IsRequested(header) {
		return key, crypto.ErrIncompatibleEncryptionMethod
	}

	k, err := crypto.SSEC.ParseHTTP(header)
	return k[:], err
}

// This function rotates old to new key.
func rotateKey(oldKey []byte, newKey []byte, bucket, object string, metadata map[string]string) error {
	switch {
	default:
		return errObjectTampered
	case crypto.SSEC.IsEncrypted(metadata):
		sealedKey, err := crypto.SSEC.ParseMetadata(metadata)
		if err != nil {
			return err
		}

		var objectKey crypto.ObjectKey
		var extKey [32]byte
		copy(extKey[:], oldKey)
		if err = objectKey.Unseal(extKey, sealedKey, crypto.SSEC.String(), bucket, object); err != nil {
			if subtle.ConstantTimeCompare(oldKey, newKey) == 1 {
				return errInvalidSSEParameters // AWS returns special error for equal but invalid keys.
			}
			return crypto.ErrInvalidCustomerKey // To provide strict AWS S3 compatibility we return: access denied.

		}
		if subtle.ConstantTimeCompare(oldKey, newKey) == 1 && sealedKey.Algorithm == crypto.SealAlgorithm {
			return nil // don't rotate on equal keys if seal algorithm is latest
		}
		copy(extKey[:], newKey)
		sealedKey = objectKey.Seal(extKey, sealedKey.IV, crypto.SSEC.String(), bucket, object)
		crypto.SSEC.CreateMetadata(metadata, sealedKey)
		return nil
	case crypto.S3.IsEncrypted(metadata):
		if GlobalKMS == nil {
			return errKMSNotConfigured
		}
		keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(metadata)
		if err != nil {
			return err
		}
		oldKey, err := GlobalKMS.UnsealKey(keyID, kmsKey, crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(oldKey, sealedKey, crypto.S3.String(), bucket, object); err != nil {
			return err
		}

		newKey, encKey, err := GlobalKMS.GenerateKey(GlobalKMS.KeyID(), crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return err
		}
		sealedKey = objectKey.Seal(newKey, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
		crypto.S3.CreateMetadata(metadata, GlobalKMS.KeyID(), encKey, sealedKey)
		return nil
	}
}

func newEncryptMetadata(key []byte, bucket, object string, metadata map[string]string, sseS3 bool) (crypto.ObjectKey, error) {
	var sealedKey crypto.SealedKey
	if sseS3 {
		if GlobalKMS == nil {
			return crypto.ObjectKey{}, errKMSNotConfigured
		}
		key, encKey, err := GlobalKMS.GenerateKey(GlobalKMS.KeyID(), crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return crypto.ObjectKey{}, err
		}

		objectKey := crypto.GenerateKey(key, rand.Reader)
		sealedKey = objectKey.Seal(key, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
		crypto.S3.CreateMetadata(metadata, GlobalKMS.KeyID(), encKey, sealedKey)
		return objectKey, nil
	}
	var extKey [32]byte
	copy(extKey[:], key)
	objectKey := crypto.GenerateKey(extKey, rand.Reader)
	sealedKey = objectKey.Seal(extKey, crypto.GenerateIV(rand.Reader), crypto.SSEC.String(), bucket, object)
	crypto.SSEC.CreateMetadata(metadata, sealedKey)
	return objectKey, nil
}

func newEncryptReader(content io.Reader, key []byte, bucket, object string, metadata map[string]string, sseS3 bool) (io.Reader, crypto.ObjectKey, error) {
	objectEncryptionKey, err := newEncryptMetadata(key, bucket, object, metadata, sseS3)
	if err != nil {
		return nil, crypto.ObjectKey{}, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20})
	if err != nil {
		return nil, crypto.ObjectKey{}, crypto.ErrInvalidCustomerKey
	}

	return reader, objectEncryptionKey, nil
}

// set new encryption metadata from http request headers for SSE-C and generated key from KMS in the case of
// SSE-S3
func setEncryptionMetadata(r *http.Request, bucket, object string, metadata map[string]string) (err error) {
	var (
		key []byte
	)
	if crypto.SSEC.IsRequested(r.Header) {
		key, err = ParseSSECustomerRequest(r)
		if err != nil {
			return
		}
	}
	_, err = newEncryptMetadata(key, bucket, object, metadata, crypto.S3.IsRequested(r.Header))
	return
}

// EncryptRequest takes the client provided content and encrypts the data
// with the client provided key. It also marks the object as client-side-encrypted
// and sets the correct headers.
func EncryptRequest(content io.Reader, r *http.Request, bucket, object string, metadata map[string]string) (io.Reader, crypto.ObjectKey, error) {
	if crypto.S3.IsRequested(r.Header) && crypto.SSEC.IsRequested(r.Header) {
		return nil, crypto.ObjectKey{}, crypto.ErrIncompatibleEncryptionMethod
	}
	if r.ContentLength > encryptBufferThreshold {
		// The encryption reads in blocks of 64KB.
		// We add a buffer on bigger files to reduce the number of syscalls upstream.
		content = bufio.NewReaderSize(content, encryptBufferSize)
	}

	var key []byte
	if crypto.SSEC.IsRequested(r.Header) {
		var err error
		key, err = ParseSSECustomerRequest(r)
		if err != nil {
			return nil, crypto.ObjectKey{}, err
		}
	}
	return newEncryptReader(content, key, bucket, object, metadata, crypto.S3.IsRequested(r.Header))
}

func decryptObjectInfo(key []byte, bucket, object string, metadata map[string]string) ([]byte, error) {
	switch {
	default:
		return nil, errObjectTampered
	case crypto.S3.IsEncrypted(metadata) && isCacheEncrypted(metadata):
		if globalCacheKMS == nil {
			return nil, errKMSNotConfigured
		}
		keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(metadata)
		if err != nil {
			return nil, err
		}
		extKey, err := globalCacheKMS.UnsealKey(keyID, kmsKey, crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return nil, err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(extKey, sealedKey, crypto.S3.String(), bucket, object); err != nil {
			return nil, err
		}
		return objectKey[:], nil
	case crypto.S3.IsEncrypted(metadata):
		if GlobalKMS == nil {
			return nil, errKMSNotConfigured
		}
		keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(metadata)

		if err != nil {
			return nil, err
		}
		extKey, err := GlobalKMS.UnsealKey(keyID, kmsKey, crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return nil, err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(extKey, sealedKey, crypto.S3.String(), bucket, object); err != nil {
			return nil, err
		}
		return objectKey[:], nil
	case crypto.SSEC.IsEncrypted(metadata):
		var extKey [32]byte
		copy(extKey[:], key)
		sealedKey, err := crypto.SSEC.ParseMetadata(metadata)
		if err != nil {
			return nil, err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(extKey, sealedKey, crypto.SSEC.String(), bucket, object); err != nil {
			return nil, err
		}
		return objectKey[:], nil
	}
}

// Adding support for reader based interface

// DecryptRequestWithSequenceNumberR - same as
// DecryptRequestWithSequenceNumber but with a reader
func DecryptRequestWithSequenceNumberR(client io.Reader, h http.Header, bucket, object string, seqNumber uint32, metadata map[string]string) (io.Reader, error) {
	if crypto.S3.IsEncrypted(metadata) {
		return newDecryptReader(client, nil, bucket, object, seqNumber, metadata)
	}

	key, err := ParseSSECustomerHeader(h)
	if err != nil {
		return nil, err
	}
	return newDecryptReader(client, key, bucket, object, seqNumber, metadata)
}

// DecryptCopyRequestR - same as DecryptCopyRequest, but with a
// Reader
func DecryptCopyRequestR(client io.Reader, h http.Header, bucket, object string, seqNumber uint32, metadata map[string]string) (io.Reader, error) {
	var (
		key []byte
		err error
	)
	if crypto.SSECopy.IsRequested(h) {
		key, err = ParseSSECopyCustomerRequest(h, metadata)
		if err != nil {
			return nil, err
		}
	}
	return newDecryptReader(client, key, bucket, object, seqNumber, metadata)
}

func newDecryptReader(client io.Reader, key []byte, bucket, object string, seqNumber uint32, metadata map[string]string) (io.Reader, error) {
	objectEncryptionKey, err := decryptObjectInfo(key, bucket, object, metadata)
	if err != nil {
		return nil, err
	}
	return newDecryptReaderWithObjectKey(client, objectEncryptionKey, seqNumber)
}

func newDecryptReaderWithObjectKey(client io.Reader, objectEncryptionKey []byte, seqNumber uint32) (io.Reader, error) {
	reader, err := sio.DecryptReader(client, sio.Config{
		Key:            objectEncryptionKey,
		SequenceNumber: seqNumber,
	})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}
	return reader, nil
}

// DecryptBlocksRequestR - same as DecryptBlocksRequest but with a
// reader
func DecryptBlocksRequestR(inputReader io.Reader, h http.Header, offset,
	length int64, seqNumber uint32, partStart int, oi ObjectInfo, copySource bool) (
	io.Reader, error) {

	bucket, object := oi.Bucket, oi.Name
	// Single part case
	if !isEncryptedMultipart(oi) {
		var reader io.Reader
		var err error
		if copySource {
			reader, err = DecryptCopyRequestR(inputReader, h, bucket, object, seqNumber, oi.UserDefined)
		} else {
			reader, err = DecryptRequestWithSequenceNumberR(inputReader, h, bucket, object, seqNumber, oi.UserDefined)
		}
		if err != nil {
			return nil, err
		}
		return reader, nil
	}

	partDecRelOffset := int64(seqNumber) * SSEDAREPackageBlockSize
	partEncRelOffset := int64(seqNumber) * (SSEDAREPackageBlockSize + SSEDAREPackageMetaSize)

	w := &DecryptBlocksReader{
		reader:            inputReader,
		startSeqNum:       seqNumber,
		partDecRelOffset:  partDecRelOffset,
		partEncRelOffset:  partEncRelOffset,
		parts:             oi.Parts,
		partIndex:         partStart,
		header:            h,
		bucket:            bucket,
		object:            object,
		customerKeyHeader: h.Get(crypto.SSECKey),
		copySource:        copySource,
	}

	w.metadata = map[string]string{}
	// Copy encryption metadata for internal use.
	for k, v := range oi.UserDefined {
		w.metadata[k] = v
	}

	if w.copySource {
		w.customerKeyHeader = h.Get(crypto.SSECopyKey)
	}

	if err := w.buildDecrypter(w.parts[w.partIndex].Number); err != nil {
		return nil, err
	}

	return w, nil
}

// DecryptBlocksReader - decrypts multipart parts, while implementing
// a io.Reader compatible interface.
type DecryptBlocksReader struct {
	// Source of the encrypted content that will be decrypted
	reader io.Reader
	// Current decrypter for the current encrypted data block
	decrypter io.Reader
	// Start sequence number
	startSeqNum uint32
	// Current part index
	partIndex int
	// Parts information
	parts          []ObjectPartInfo
	header         http.Header
	bucket, object string
	metadata       map[string]string

	partDecRelOffset, partEncRelOffset int64

	copySource bool
	// Customer Key
	customerKeyHeader string
}

func (d *DecryptBlocksReader) buildDecrypter(partID int) error {
	m := make(map[string]string)
	for k, v := range d.metadata {
		m[k] = v
	}
	// Initialize the first decrypter; new decrypters will be
	// initialized in Read() operation as needed.
	var key []byte
	var err error
	if d.copySource {
		if crypto.SSEC.IsEncrypted(d.metadata) {
			d.header.Set(crypto.SSECopyKey, d.customerKeyHeader)
			key, err = ParseSSECopyCustomerRequest(d.header, d.metadata)
		}
	} else {
		if crypto.SSEC.IsEncrypted(d.metadata) {
			d.header.Set(crypto.SSECKey, d.customerKeyHeader)
			key, err = ParseSSECustomerHeader(d.header)
		}
	}
	if err != nil {
		return err
	}

	objectEncryptionKey, err := decryptObjectInfo(key, d.bucket, d.object, m)
	if err != nil {
		return err
	}

	var partIDbin [4]byte
	binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

	mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
	mac.Write(partIDbin[:])
	partEncryptionKey := mac.Sum(nil)

	// Limit the reader, so the decryptor doesnt receive bytes
	// from the next part (different DARE stream)
	encLenToRead := d.parts[d.partIndex].Size - d.partEncRelOffset
	decrypter, err := newDecryptReaderWithObjectKey(io.LimitReader(d.reader, encLenToRead), partEncryptionKey, d.startSeqNum)
	if err != nil {
		return err
	}

	d.decrypter = decrypter
	return nil
}

func (d *DecryptBlocksReader) Read(p []byte) (int, error) {
	var err error
	var n1 int
	decPartSize, _ := sio.DecryptedSize(uint64(d.parts[d.partIndex].Size))
	unreadPartLen := int64(decPartSize) - d.partDecRelOffset
	if int64(len(p)) < unreadPartLen {
		n1, err = d.decrypter.Read(p)
		if err != nil {
			return 0, err
		}
		d.partDecRelOffset += int64(n1)
	} else {
		n1, err = io.ReadFull(d.decrypter, p[:unreadPartLen])
		if err != nil {
			return 0, err
		}

		// We should now proceed to next part, reset all
		// values appropriately.
		d.partEncRelOffset = 0
		d.partDecRelOffset = 0
		d.startSeqNum = 0

		d.partIndex++
		if d.partIndex == len(d.parts) {
			return n1, io.EOF
		}

		err = d.buildDecrypter(d.parts[d.partIndex].Number)
		if err != nil {
			return 0, err
		}

		n1, err = d.decrypter.Read(p[n1:])
		if err != nil {
			return 0, err
		}

		d.partDecRelOffset += int64(n1)
	}
	return len(p), nil
}

// DecryptedSize returns the size of the object after decryption in bytes.
// It returns an error if the object is not encrypted or marked as encrypted
// but has an invalid size.
func (o *ObjectInfo) DecryptedSize() (int64, error) {
	if !crypto.IsEncrypted(o.UserDefined) {
		return 0, errors.New("Cannot compute decrypted size of an unencrypted object")
	}
	if !isEncryptedMultipart(*o) {
		size, err := sio.DecryptedSize(uint64(o.Size))
		if err != nil {
			err = errObjectTampered // assign correct error type
		}
		return int64(size), err
	}

	var size int64
	for _, part := range o.Parts {
		partSize, err := sio.DecryptedSize(uint64(part.Size))
		if err != nil {
			return 0, errObjectTampered
		}
		size += int64(partSize)
	}
	return size, nil
}

// DecryptETag decrypts the ETag that is part of given object
// with the given object encryption key.
//
// However, DecryptETag does not try to decrypt the ETag if
// it consists of a 128 bit hex value (32 hex chars) and exactly
// one '-' followed by a 32-bit number.
// This special case adresses randomly-generated ETags generated
// by the MinIO server when running in non-compat mode. These
// random ETags are not encrypt.
//
// Calling DecryptETag with a non-randomly generated ETag will
// fail.
func DecryptETag(key crypto.ObjectKey, object ObjectInfo) (string, error) {
	if n := strings.Count(object.ETag, "-"); n > 0 {
		if n != 1 {
			return "", errObjectTampered
		}
		i := strings.IndexByte(object.ETag, '-')
		if len(object.ETag[:i]) != 32 {
			return "", errObjectTampered
		}
		if _, err := hex.DecodeString(object.ETag[:32]); err != nil {
			return "", errObjectTampered
		}
		if _, err := strconv.ParseInt(object.ETag[i+1:], 10, 32); err != nil {
			return "", errObjectTampered
		}
		return object.ETag, nil
	}

	etag, err := hex.DecodeString(object.ETag)
	if err != nil {
		return "", err
	}
	etag, err = key.UnsealETag(etag)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(etag), nil
}

// For encrypted objects, the ETag sent by client if available
// is stored in encrypted form in the backend. Decrypt the ETag
// if ETag was previously encrypted.
func getDecryptedETag(headers http.Header, objInfo ObjectInfo, copySource bool) (decryptedETag string) {
	var (
		key [32]byte
		err error
	)
	// If ETag is contentMD5Sum return it as is.
	if len(objInfo.ETag) == 32 {
		return objInfo.ETag
	}

	if crypto.IsMultiPart(objInfo.UserDefined) {
		return objInfo.ETag
	}
	if crypto.SSECopy.IsRequested(headers) {
		key, err = crypto.SSECopy.ParseHTTP(headers)
		if err != nil {
			return objInfo.ETag
		}
	}
	// As per AWS S3 Spec, ETag for SSE-C encrypted objects need not be MD5Sum of the data.
	// Since server side copy with same source and dest just replaces the ETag, we save
	// encrypted content MD5Sum as ETag for both SSE-C and SSE-S3, we standardize the ETag
	//encryption across SSE-C and SSE-S3, and only return last 32 bytes for SSE-C
	if crypto.SSEC.IsEncrypted(objInfo.UserDefined) && !copySource {
		return objInfo.ETag[len(objInfo.ETag)-32:]
	}

	objectEncryptionKey, err := decryptObjectInfo(key[:], objInfo.Bucket, objInfo.Name, objInfo.UserDefined)
	if err != nil {
		return objInfo.ETag
	}
	return tryDecryptETag(objectEncryptionKey, objInfo.ETag, false)
}

// helper to decrypt Etag given object encryption key and encrypted ETag
func tryDecryptETag(key []byte, encryptedETag string, ssec bool) string {
	// ETag for SSE-C encrypted objects need not be content MD5Sum.While encrypted
	// md5sum is stored internally, return just the last 32 bytes of hex-encoded and
	// encrypted md5sum string for SSE-C
	if ssec {
		return encryptedETag[len(encryptedETag)-32:]
	}
	var objectKey crypto.ObjectKey
	copy(objectKey[:], key)
	encBytes, err := hex.DecodeString(encryptedETag)
	if err != nil {
		return encryptedETag
	}
	etagBytes, err := objectKey.UnsealETag(encBytes)
	if err != nil {
		return encryptedETag
	}
	return hex.EncodeToString(etagBytes)
}

// GetDecryptedRange - To decrypt the range (off, length) of the
// decrypted object stream, we need to read the range (encOff,
// encLength) of the encrypted object stream to decrypt it, and
// compute skipLen, the number of bytes to skip in the beginning of
// the encrypted range.
//
// In addition we also compute the object part number for where the
// requested range starts, along with the DARE sequence number within
// that part. For single part objects, the partStart will be 0.
func (o *ObjectInfo) GetDecryptedRange(rs *HTTPRangeSpec) (encOff, encLength, skipLen int64, seqNumber uint32, partStart int, err error) {
	if !crypto.IsEncrypted(o.UserDefined) {
		err = errors.New("Object is not encrypted")
		return
	}

	if rs == nil {
		// No range, so offsets refer to the whole object.
		return 0, int64(o.Size), 0, 0, 0, nil
	}

	// Assemble slice of (decrypted) part sizes in `sizes`
	var sizes []int64
	var decObjSize int64 // decrypted total object size
	if isEncryptedMultipart(*o) {
		sizes = make([]int64, len(o.Parts))
		for i, part := range o.Parts {
			var partSize uint64
			partSize, err = sio.DecryptedSize(uint64(part.Size))
			if err != nil {
				err = errObjectTampered
				return
			}
			sizes[i] = int64(partSize)
			decObjSize += int64(partSize)
		}
	} else {
		var partSize uint64
		partSize, err = sio.DecryptedSize(uint64(o.Size))
		if err != nil {
			err = errObjectTampered
			return
		}
		sizes = []int64{int64(partSize)}
		decObjSize = sizes[0]
	}

	var off, length int64
	off, length, err = rs.GetOffsetLength(decObjSize)
	if err != nil {
		return
	}

	// At this point, we have:
	//
	// 1. the decrypted part sizes in `sizes` (single element for
	//    single part object) and total decrypted object size `decObjSize`
	//
	// 2. the (decrypted) start offset `off` and (decrypted)
	//    length to read `length`
	//
	// These are the inputs to the rest of the algorithm below.

	// Locate the part containing the start of the required range
	var partEnd int
	var cumulativeSum, encCumulativeSum int64
	for i, size := range sizes {
		if off < cumulativeSum+size {
			partStart = i
			break
		}
		cumulativeSum += size
		encPartSize, _ := sio.EncryptedSize(uint64(size))
		encCumulativeSum += int64(encPartSize)
	}
	// partStart is always found in the loop above,
	// because off is validated.

	sseDAREEncPackageBlockSize := int64(SSEDAREPackageBlockSize + SSEDAREPackageMetaSize)
	startPkgNum := (off - cumulativeSum) / SSEDAREPackageBlockSize

	// Now we can calculate the number of bytes to skip
	skipLen = (off - cumulativeSum) % SSEDAREPackageBlockSize

	encOff = encCumulativeSum + startPkgNum*sseDAREEncPackageBlockSize
	// Locate the part containing the end of the required range
	endOffset := off + length - 1
	for i1, size := range sizes[partStart:] {
		i := partStart + i1
		if endOffset < cumulativeSum+size {
			partEnd = i
			break
		}
		cumulativeSum += size
		encPartSize, _ := sio.EncryptedSize(uint64(size))
		encCumulativeSum += int64(encPartSize)
	}
	// partEnd is always found in the loop above, because off and
	// length are validated.
	endPkgNum := (endOffset - cumulativeSum) / SSEDAREPackageBlockSize
	// Compute endEncOffset with one additional DARE package (so
	// we read the package containing the last desired byte).
	endEncOffset := encCumulativeSum + (endPkgNum+1)*sseDAREEncPackageBlockSize
	// Check if the DARE package containing the end offset is a
	// full sized package (as the last package in the part may be
	// smaller)
	lastPartSize, _ := sio.EncryptedSize(uint64(sizes[partEnd]))
	if endEncOffset > encCumulativeSum+int64(lastPartSize) {
		endEncOffset = encCumulativeSum + int64(lastPartSize)
	}
	encLength = endEncOffset - encOff
	// Set the sequence number as the starting package number of
	// the requested block
	seqNumber = uint32(startPkgNum)
	return encOff, encLength, skipLen, seqNumber, partStart, nil
}

// EncryptedSize returns the size of the object after encryption.
// An encrypted object is always larger than a plain object
// except for zero size objects.
func (o *ObjectInfo) EncryptedSize() int64 {
	size, err := sio.EncryptedSize(uint64(o.Size))
	if err != nil {
		// This cannot happen since AWS S3 allows parts to be 5GB at most
		// sio max. size is 256 TB
		reqInfo := (&logger.ReqInfo{}).AppendTags("size", strconv.FormatUint(size, 10))
		ctx := logger.SetReqInfo(GlobalContext, reqInfo)
		logger.CriticalIf(ctx, err)
	}
	return int64(size)
}

// DecryptCopyObjectInfo tries to decrypt the provided object if it is encrypted.
// It fails if the object is encrypted and the HTTP headers don't contain
// SSE-C headers or the object is not encrypted but SSE-C headers are provided. (AWS behavior)
// DecryptObjectInfo returns 'ErrNone' if the object is not encrypted or the
// decryption succeeded.
//
// DecryptCopyObjectInfo also returns whether the object is encrypted or not.
func DecryptCopyObjectInfo(info *ObjectInfo, headers http.Header) (errCode APIErrorCode, encrypted bool) {
	// Directories are never encrypted.
	if info.IsDir {
		return ErrNone, false
	}
	if errCode, encrypted = ErrNone, crypto.IsEncrypted(info.UserDefined); !encrypted && crypto.SSECopy.IsRequested(headers) {
		errCode = ErrInvalidEncryptionParameters
	} else if encrypted {
		if (!crypto.SSECopy.IsRequested(headers) && crypto.SSEC.IsEncrypted(info.UserDefined)) ||
			(crypto.SSECopy.IsRequested(headers) && crypto.S3.IsEncrypted(info.UserDefined)) {
			errCode = ErrSSEEncryptedObject
			return
		}
		var err error
		if info.Size, err = info.DecryptedSize(); err != nil {
			errCode = toAPIErrorCode(GlobalContext, err)
		}
	}
	return
}

// DecryptObjectInfo tries to decrypt the provided object if it is encrypted.
// It fails if the object is encrypted and the HTTP headers don't contain
// SSE-C headers or the object is not encrypted but SSE-C headers are provided. (AWS behavior)
// DecryptObjectInfo returns 'ErrNone' if the object is not encrypted or the
// decryption succeeded.
//
// DecryptObjectInfo also returns whether the object is encrypted or not.
func DecryptObjectInfo(info *ObjectInfo, headers http.Header) (encrypted bool, err error) {
	// Directories are never encrypted.
	if info.IsDir {
		return false, nil
	}
	// disallow X-Amz-Server-Side-Encryption header on HEAD and GET
	if crypto.S3.IsRequested(headers) {
		err = errInvalidEncryptionParameters
		return
	}
	if err, encrypted = nil, crypto.IsEncrypted(info.UserDefined); !encrypted && crypto.SSEC.IsRequested(headers) {
		err = errInvalidEncryptionParameters
	} else if encrypted {
		if (crypto.SSEC.IsEncrypted(info.UserDefined) && !crypto.SSEC.IsRequested(headers)) ||
			(crypto.S3.IsEncrypted(info.UserDefined) && crypto.SSEC.IsRequested(headers)) {
			err = errEncryptedObject
			return
		}
		_, err = info.DecryptedSize()

		if crypto.IsEncrypted(info.UserDefined) && !crypto.IsMultiPart(info.UserDefined) {
			info.ETag = getDecryptedETag(headers, *info, false)
		}

	}
	return
}

// The customer key in the header is used by the gateway for encryption in the case of
// s3 gateway double encryption. A new client key is derived from the customer provided
// key to be sent to the s3 backend for encryption at the backend.
func deriveClientKey(clientKey [32]byte, bucket, object string) [32]byte {
	var key [32]byte
	mac := hmac.New(sha256.New, clientKey[:])
	mac.Write([]byte(crypto.SSEC.String()))
	mac.Write([]byte(path.Join(bucket, object)))
	mac.Sum(key[:0])
	return key
}

// set encryption options for pass through to backend in the case of gateway and UserDefined metadata
func getDefaultOpts(header http.Header, copySource bool, metadata map[string]string) (opts ObjectOptions, err error) {
	var clientKey [32]byte
	var sse encrypt.ServerSide

	opts = ObjectOptions{UserDefined: metadata}
	if copySource {
		if crypto.SSECopy.IsRequested(header) {
			clientKey, err = crypto.SSECopy.ParseHTTP(header)
			if err != nil {
				return
			}
			if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
				return
			}
			opts.ServerSideEncryption = encrypt.SSECopy(sse)
			return
		}
		return
	}

	if crypto.SSEC.IsRequested(header) {
		clientKey, err = crypto.SSEC.ParseHTTP(header)
		if err != nil {
			return
		}
		if sse, err = encrypt.NewSSEC(clientKey[:]); err != nil {
			return
		}
		opts.ServerSideEncryption = sse
		return
	}
	if crypto.S3.IsRequested(header) || (metadata != nil && crypto.S3.IsEncrypted(metadata)) {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	return
}

// get ObjectOptions for GET calls from encryption headers
func getOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var (
		encryption encrypt.ServerSide
		opts       ObjectOptions
	)

	var partNumber int
	var err error
	if pn := r.URL.Query().Get("partNumber"); pn != "" {
		partNumber, err = strconv.Atoi(pn)
		if err != nil {
			return opts, err
		}
		if partNumber <= 0 {
			return opts, errInvalidArgument
		}
	}

	vid := strings.TrimSpace(r.URL.Query().Get("versionId"))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, VersionNotFound{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
	}

	if GlobalGatewaySSE.SSEC() && crypto.SSEC.IsRequested(r.Header) {
		key, err := crypto.SSEC.ParseHTTP(r.Header)
		if err != nil {
			return opts, err
		}
		derivedKey := deriveClientKey(key, bucket, object)
		encryption, err = encrypt.NewSSEC(derivedKey[:])
		logger.CriticalIf(ctx, err)
		return ObjectOptions{
			ServerSideEncryption: encryption,
			VersionID:            vid,
			PartNumber:           partNumber,
		}, nil
	}

	// default case of passing encryption headers to backend
	opts, err = getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	opts.PartNumber = partNumber
	opts.VersionID = vid
	return opts, nil
}

func delOpts(ctx context.Context, r *http.Request, bucket, object string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.Enabled(bucket)
	opts, err = getOpts(ctx, r, bucket, object)
	if err != nil {
		return opts, err
	}
	opts.Versioned = versioned
	return opts, nil
}

// get ObjectOptions for PUT calls from encryption headers and metadata
func putOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	versioned := globalBucketVersioningSys.Enabled(bucket)
	vid := strings.TrimSpace(r.URL.Query().Get("versionId"))
	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			logger.LogIf(ctx, err)
			return opts, VersionNotFound{
				Bucket:    bucket,
				Object:    object,
				VersionID: vid,
			}
		}
	}

	// In the case of multipart custom format, the metadata needs to be checked in addition to header to see if it
	// is SSE-S3 encrypted, primarily because S3 protocol does not require SSE-S3 headers in PutObjectPart calls
	if GlobalGatewaySSE.SSES3() && (crypto.S3.IsRequested(r.Header) || crypto.S3.IsEncrypted(metadata)) {
		return ObjectOptions{
			ServerSideEncryption: encrypt.NewSSE(),
			UserDefined:          metadata,
			VersionID:            vid,
			Versioned:            versioned,
		}, nil
	}
	if GlobalGatewaySSE.SSEC() && crypto.SSEC.IsRequested(r.Header) {
		opts, err = getOpts(ctx, r, bucket, object)
		opts.VersionID = vid
		opts.Versioned = versioned
		opts.UserDefined = metadata
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		keyID, context, err := crypto.S3KMS.ParseHTTP(r.Header)
		if err != nil {
			return ObjectOptions{}, err
		}
		sseKms, err := encrypt.NewSSEKMS(keyID, context)
		if err != nil {
			return ObjectOptions{}, err
		}
		return ObjectOptions{
			ServerSideEncryption: sseKms,
			UserDefined:          metadata,
			VersionID:            vid,
			Versioned:            versioned,
		}, nil
	}
	// default case of passing encryption headers and UserDefined metadata to backend
	opts, err = getDefaultOpts(r.Header, false, metadata)
	if err != nil {
		return opts, err
	}
	opts.VersionID = vid
	opts.Versioned = versioned
	return opts, nil
}

// get ObjectOptions for Copy calls with encryption headers provided on the target side and source side metadata
func copyDstOpts(ctx context.Context, r *http.Request, bucket, object string, metadata map[string]string) (opts ObjectOptions, err error) {
	return putOpts(ctx, r, bucket, object, metadata)
}

// get ObjectOptions for Copy calls with encryption headers provided on the source side
func copySrcOpts(ctx context.Context, r *http.Request, bucket, object string) (ObjectOptions, error) {
	var (
		ssec encrypt.ServerSide
		opts ObjectOptions
	)

	if GlobalGatewaySSE.SSEC() && crypto.SSECopy.IsRequested(r.Header) {
		key, err := crypto.SSECopy.ParseHTTP(r.Header)
		if err != nil {
			return opts, err
		}
		derivedKey := deriveClientKey(key, bucket, object)
		ssec, err = encrypt.NewSSEC(derivedKey[:])
		if err != nil {
			return opts, err
		}
		return ObjectOptions{ServerSideEncryption: encrypt.SSECopy(ssec)}, nil
	}

	// default case of passing encryption headers to backend
	opts, err := getDefaultOpts(r.Header, false, nil)
	if err != nil {
		return opts, err
	}
	return opts, nil
}
