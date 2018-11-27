/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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

	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/ioutil"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

var (
	// AWS errors for invalid SSE-C requests.
	errEncryptedObject      = errors.New("The object was stored using a form of SSE")
	errInvalidSSEParameters = errors.New("The SSE-C key for key-rotation is not correct") // special access denied
	errKMSNotConfigured     = errors.New("KMS not configured for a server side encrypted object")
	// Additional Minio errors for SSE-C requests.
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

	// SSE dare package block size.
	sseDAREPackageBlockSize = 64 * 1024 // 64KiB bytes

	// SSE dare package meta padding bytes.
	sseDAREPackageMetaSize = 32 // 32 bytes

)

const (
	// SSESealAlgorithmDareSha256 specifies DARE as authenticated en/decryption scheme and SHA256 as cryptographic
	// hash function. The key derivation of DARE-SHA256 is not optimal and does not include the object path.
	// It is considered legacy and should not be used anymore.
	SSESealAlgorithmDareSha256 = "DARE-SHA256"

	// SSESealAlgorithmDareV2HmacSha256 specifies DAREv2 as authenticated en/decryption scheme and SHA256 as cryptographic
	// hash function for the HMAC PRF.
	SSESealAlgorithmDareV2HmacSha256 = "DAREv2-HMAC-SHA256"
)

// hasServerSideEncryptionHeader returns true if the given HTTP header
// contains server-side-encryption.
func hasServerSideEncryptionHeader(header http.Header) bool {
	return crypto.S3.IsRequested(header) || crypto.SSEC.IsRequested(header)
}

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
	// by the user and it is not about XL internally splitting the
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
		if globalKMS == nil {
			return errKMSNotConfigured
		}
		keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(metadata)
		if err != nil {
			return err
		}
		oldKey, err := globalKMS.UnsealKey(keyID, kmsKey, crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(oldKey, sealedKey, crypto.S3.String(), bucket, object); err != nil {
			return err
		}

		newKey, encKey, err := globalKMS.GenerateKey(globalKMSKeyID, crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return err
		}
		sealedKey = objectKey.Seal(newKey, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
		crypto.S3.CreateMetadata(metadata, globalKMSKeyID, encKey, sealedKey)
		return nil
	}
}

func newEncryptMetadata(key []byte, bucket, object string, metadata map[string]string, sseS3 bool) ([]byte, error) {
	var sealedKey crypto.SealedKey
	if sseS3 {
		if globalKMS == nil {
			return nil, errKMSNotConfigured
		}
		key, encKey, err := globalKMS.GenerateKey(globalKMSKeyID, crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return nil, err
		}

		objectKey := crypto.GenerateKey(key, rand.Reader)
		sealedKey = objectKey.Seal(key, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
		crypto.S3.CreateMetadata(metadata, globalKMSKeyID, encKey, sealedKey)
		return objectKey[:], nil
	}
	var extKey [32]byte
	copy(extKey[:], key)
	objectKey := crypto.GenerateKey(extKey, rand.Reader)
	sealedKey = objectKey.Seal(extKey, crypto.GenerateIV(rand.Reader), crypto.SSEC.String(), bucket, object)
	crypto.SSEC.CreateMetadata(metadata, sealedKey)
	return objectKey[:], nil
}

func newEncryptReader(content io.Reader, key []byte, bucket, object string, metadata map[string]string, sseS3 bool) (r io.Reader, encKey []byte, err error) {
	objectEncryptionKey, err := newEncryptMetadata(key, bucket, object, metadata, sseS3)
	if err != nil {
		return nil, encKey, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20})
	if err != nil {
		return nil, encKey, crypto.ErrInvalidCustomerKey
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
func EncryptRequest(content io.Reader, r *http.Request, bucket, object string, metadata map[string]string) (reader io.Reader, objEncKey []byte, err error) {

	var (
		key []byte
	)
	if crypto.S3.IsRequested(r.Header) && crypto.SSEC.IsRequested(r.Header) {
		return nil, objEncKey, crypto.ErrIncompatibleEncryptionMethod
	}
	if crypto.SSEC.IsRequested(r.Header) {
		key, err = ParseSSECustomerRequest(r)
		if err != nil {
			return nil, objEncKey, err
		}
	}
	return newEncryptReader(content, key, bucket, object, metadata, crypto.S3.IsRequested(r.Header))
}

// DecryptCopyRequest decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptCopyRequest(client io.Writer, r *http.Request, bucket, object string, metadata map[string]string) (io.WriteCloser, error) {
	var (
		key []byte
		err error
	)
	if crypto.SSECopy.IsRequested(r.Header) {
		key, err = ParseSSECopyCustomerRequest(r.Header, metadata)
		if err != nil {
			return nil, err
		}
	}
	return newDecryptWriter(client, key, bucket, object, 0, metadata)
}

func decryptObjectInfo(key []byte, bucket, object string, metadata map[string]string) ([]byte, error) {
	switch {
	default:
		return nil, errObjectTampered
	case crypto.S3.IsEncrypted(metadata):
		if globalKMS == nil {
			return nil, errKMSNotConfigured
		}
		keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(metadata)

		if err != nil {
			return nil, err
		}
		extKey, err := globalKMS.UnsealKey(keyID, kmsKey, crypto.Context{bucket: path.Join(bucket, object)})
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

func newDecryptWriter(client io.Writer, key []byte, bucket, object string, seqNumber uint32, metadata map[string]string) (io.WriteCloser, error) {
	objectEncryptionKey, err := decryptObjectInfo(key, bucket, object, metadata)
	if err != nil {
		return nil, err
	}
	return newDecryptWriterWithObjectKey(client, objectEncryptionKey, seqNumber, metadata)
}

func newDecryptWriterWithObjectKey(client io.Writer, objectEncryptionKey []byte, seqNumber uint32, metadata map[string]string) (io.WriteCloser, error) {
	writer, err := sio.DecryptWriter(client, sio.Config{
		Key:            objectEncryptionKey,
		SequenceNumber: seqNumber,
	})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}
	delete(metadata, crypto.SSEIV)
	delete(metadata, crypto.SSESealAlgorithm)
	delete(metadata, crypto.SSECSealedKey)
	delete(metadata, crypto.SSEMultipart)
	delete(metadata, crypto.S3SealedKey)
	delete(metadata, crypto.S3KMSSealedKey)
	delete(metadata, crypto.S3KMSKeyID)
	return writer, nil
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
	return newDecryptReaderWithObjectKey(client, objectEncryptionKey, seqNumber, metadata)
}

func newDecryptReaderWithObjectKey(client io.Reader, objectEncryptionKey []byte, seqNumber uint32, metadata map[string]string) (io.Reader, error) {
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

	partDecRelOffset := int64(seqNumber) * sseDAREPackageBlockSize
	partEncRelOffset := int64(seqNumber) * (sseDAREPackageBlockSize + sseDAREPackageMetaSize)

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

// DecryptRequestWithSequenceNumber decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptRequestWithSequenceNumber(client io.Writer, r *http.Request, bucket, object string, seqNumber uint32, metadata map[string]string) (io.WriteCloser, error) {
	if crypto.S3.IsEncrypted(metadata) {
		return newDecryptWriter(client, nil, bucket, object, seqNumber, metadata)
	}

	key, err := ParseSSECustomerRequest(r)
	if err != nil {
		return nil, err
	}
	return newDecryptWriter(client, key, bucket, object, seqNumber, metadata)
}

// DecryptRequest decrypts the object with client provided key for SSE-C and SSE-S3. It also removes
// the encryption metadata from the object and sets the correct headers.
func DecryptRequest(client io.Writer, r *http.Request, bucket, object string, metadata map[string]string) (io.WriteCloser, error) {
	return DecryptRequestWithSequenceNumber(client, r, bucket, object, 0, metadata)
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
	parts          []objectPartInfo
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
	decrypter, err := newDecryptReaderWithObjectKey(io.LimitReader(d.reader, encLenToRead), partEncryptionKey, d.startSeqNum, m)
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

// DecryptBlocksWriter - decrypts  multipart parts, while implementing
// a io.Writer compatible interface.
type DecryptBlocksWriter struct {
	// Original writer where the plain data will be written
	writer io.Writer
	// Current decrypter for the current encrypted data block
	decrypter io.WriteCloser
	// Start sequence number
	startSeqNum uint32
	// Current part index
	partIndex int
	// Parts information
	parts          []objectPartInfo
	req            *http.Request
	bucket, object string
	metadata       map[string]string

	partEncRelOffset int64

	copySource bool
	// Customer Key
	customerKeyHeader string
}

func (w *DecryptBlocksWriter) buildDecrypter(partID int) error {
	m := make(map[string]string)
	for k, v := range w.metadata {
		m[k] = v
	}
	// Initialize the first decrypter, new decrypters will be initialized in Write() operation as needed.
	var key []byte
	var err error
	if w.copySource {
		if crypto.SSEC.IsEncrypted(w.metadata) {
			w.req.Header.Set(crypto.SSECopyKey, w.customerKeyHeader)
			key, err = ParseSSECopyCustomerRequest(w.req.Header, w.metadata)
		}
	} else {
		if crypto.SSEC.IsEncrypted(w.metadata) {
			w.req.Header.Set(crypto.SSECKey, w.customerKeyHeader)
			key, err = ParseSSECustomerRequest(w.req)
		}
	}
	if err != nil {
		return err
	}

	objectEncryptionKey, err := decryptObjectInfo(key, w.bucket, w.object, m)
	if err != nil {
		return err
	}

	var partIDbin [4]byte
	binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

	mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
	mac.Write(partIDbin[:])
	partEncryptionKey := mac.Sum(nil)

	// make sure to provide a NopCloser such that a Close
	// on sio.decryptWriter doesn't close the underlying writer's
	// close which perhaps can close the stream prematurely.
	decrypter, err := newDecryptWriterWithObjectKey(ioutil.NopCloser(w.writer), partEncryptionKey, w.startSeqNum, m)
	if err != nil {
		return err
	}

	if w.decrypter != nil {
		// Pro-actively close the writer such that any pending buffers
		// are flushed already before we allocate a new decrypter.
		err = w.decrypter.Close()
		if err != nil {
			return err
		}
	}

	w.decrypter = decrypter
	return nil
}

func (w *DecryptBlocksWriter) Write(p []byte) (int, error) {
	var err error
	var n1 int
	if int64(len(p)) < w.parts[w.partIndex].Size-w.partEncRelOffset {
		n1, err = w.decrypter.Write(p)
		if err != nil {
			return 0, err
		}
		w.partEncRelOffset += int64(n1)
	} else {
		n1, err = w.decrypter.Write(p[:w.parts[w.partIndex].Size-w.partEncRelOffset])
		if err != nil {
			return 0, err
		}

		// We should now proceed to next part, reset all values appropriately.
		w.partEncRelOffset = 0
		w.startSeqNum = 0

		w.partIndex++

		err = w.buildDecrypter(w.partIndex + 1)
		if err != nil {
			return 0, err
		}

		n1, err = w.decrypter.Write(p[n1:])
		if err != nil {
			return 0, err
		}

		w.partEncRelOffset += int64(n1)
	}

	return len(p), nil
}

// Close closes the LimitWriter. It behaves like io.Closer.
func (w *DecryptBlocksWriter) Close() error {
	if w.decrypter != nil {
		err := w.decrypter.Close()
		if err != nil {
			return err
		}
	}

	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// DecryptAllBlocksCopyRequest - setup a struct which can decrypt many concatenated encrypted data
// parts information helps to know the boundaries of each encrypted data block, this function decrypts
// all parts starting from part-1.
func DecryptAllBlocksCopyRequest(client io.Writer, r *http.Request, bucket, object string, objInfo ObjectInfo) (io.WriteCloser, int64, error) {
	w, _, size, err := DecryptBlocksRequest(client, r, bucket, object, 0, objInfo.Size, objInfo, true)
	return w, size, err
}

// DecryptBlocksRequest - setup a struct which can decrypt many concatenated encrypted data
// parts information helps to know the boundaries of each encrypted data block.
func DecryptBlocksRequest(client io.Writer, r *http.Request, bucket, object string, startOffset, length int64, objInfo ObjectInfo, copySource bool) (io.WriteCloser, int64, int64, error) {
	var seqNumber uint32
	var encStartOffset, encLength int64

	if !isEncryptedMultipart(objInfo) {
		seqNumber, encStartOffset, encLength = getEncryptedSinglePartOffsetLength(startOffset, length, objInfo)

		var writer io.WriteCloser
		var err error
		if copySource {
			writer, err = DecryptCopyRequest(client, r, bucket, object, objInfo.UserDefined)
		} else {
			writer, err = DecryptRequestWithSequenceNumber(client, r, bucket, object, seqNumber, objInfo.UserDefined)
		}
		if err != nil {
			return nil, 0, 0, err
		}
		return writer, encStartOffset, encLength, nil
	}

	seqNumber, encStartOffset, encLength = getEncryptedMultipartsOffsetLength(startOffset, length, objInfo)
	var partStartIndex int
	var partStartOffset = startOffset
	// Skip parts until final offset maps to a particular part offset.
	for i, part := range objInfo.Parts {
		decryptedSize, err := sio.DecryptedSize(uint64(part.Size))
		if err != nil {
			return nil, -1, -1, errObjectTampered
		}

		partStartIndex = i

		// Offset is smaller than size we have reached the
		// proper part offset, break out we start from
		// this part index.
		if partStartOffset < int64(decryptedSize) {
			break
		}

		// Continue to look for next part.
		partStartOffset -= int64(decryptedSize)
	}

	startSeqNum := partStartOffset / sseDAREPackageBlockSize
	partEncRelOffset := int64(startSeqNum) * (sseDAREPackageBlockSize + sseDAREPackageMetaSize)

	w := &DecryptBlocksWriter{
		writer:            client,
		startSeqNum:       uint32(startSeqNum),
		partEncRelOffset:  partEncRelOffset,
		parts:             objInfo.Parts,
		partIndex:         partStartIndex,
		req:               r,
		bucket:            bucket,
		object:            object,
		customerKeyHeader: r.Header.Get(crypto.SSECKey),
		copySource:        copySource,
	}

	w.metadata = map[string]string{}
	// Copy encryption metadata for internal use.
	for k, v := range objInfo.UserDefined {
		w.metadata[k] = v
	}

	// Purge all the encryption headers.
	delete(objInfo.UserDefined, crypto.SSEIV)
	delete(objInfo.UserDefined, crypto.SSESealAlgorithm)
	delete(objInfo.UserDefined, crypto.SSECSealedKey)
	delete(objInfo.UserDefined, crypto.SSEMultipart)

	if crypto.S3.IsEncrypted(objInfo.UserDefined) {
		delete(objInfo.UserDefined, crypto.S3SealedKey)
		delete(objInfo.UserDefined, crypto.S3KMSKeyID)
		delete(objInfo.UserDefined, crypto.S3KMSSealedKey)
	}
	if w.copySource {
		w.customerKeyHeader = r.Header.Get(crypto.SSECopyKey)
	}

	if err := w.buildDecrypter(w.parts[w.partIndex].Number); err != nil {
		return nil, 0, 0, err
	}

	return w, encStartOffset, encLength, nil
}

// getEncryptedMultipartsOffsetLength - fetch sequence number, encrypted start offset and encrypted length.
func getEncryptedMultipartsOffsetLength(offset, length int64, obj ObjectInfo) (uint32, int64, int64) {
	// Calculate encrypted offset of a multipart object
	computeEncOffset := func(off int64, obj ObjectInfo) (seqNumber uint32, encryptedOffset int64, err error) {
		var curPartEndOffset uint64
		var prevPartsEncSize int64
		for _, p := range obj.Parts {
			size, decErr := sio.DecryptedSize(uint64(p.Size))
			if decErr != nil {
				err = errObjectTampered // assign correct error type
				return
			}
			if off < int64(curPartEndOffset+size) {
				seqNumber, encryptedOffset, _ = getEncryptedSinglePartOffsetLength(off-int64(curPartEndOffset), 1, obj)
				encryptedOffset += int64(prevPartsEncSize)
				break
			}
			curPartEndOffset += size
			prevPartsEncSize += p.Size
		}
		return
	}

	// Calculate the encrypted start offset corresponding to the plain offset
	seqNumber, encStartOffset, _ := computeEncOffset(offset, obj)
	// Calculate also the encrypted end offset corresponding to plain offset + plain length
	_, encEndOffset, _ := computeEncOffset(offset+length-1, obj)

	// encLength is the diff between encrypted end offset and encrypted start offset + one package size
	// to ensure all encrypted data are covered
	encLength := encEndOffset - encStartOffset + (64*1024 + 32)

	// Calculate total size of all parts
	var totalPartsLength int64
	for _, p := range obj.Parts {
		totalPartsLength += p.Size
	}

	// Set encLength to maximum possible value if it exceeded total parts size
	if encLength+encStartOffset > totalPartsLength {
		encLength = totalPartsLength - encStartOffset
	}

	return seqNumber, encStartOffset, encLength
}

// getEncryptedSinglePartOffsetLength - fetch sequence number, encrypted start offset and encrypted length.
func getEncryptedSinglePartOffsetLength(offset, length int64, objInfo ObjectInfo) (seqNumber uint32, encOffset int64, encLength int64) {
	onePkgSize := int64(sseDAREPackageBlockSize + sseDAREPackageMetaSize)

	seqNumber = uint32(offset / sseDAREPackageBlockSize)
	encOffset = int64(seqNumber) * onePkgSize
	// The math to compute the encrypted length is always
	// originalLength i.e (offset+length-1) to be divided under
	// 64KiB blocks which is the payload size for each encrypted
	// block. This is then multiplied by final package size which
	// is basically 64KiB + 32. Finally negate the encrypted offset
	// to get the final encrypted length on disk.
	encLength = ((offset+length)/sseDAREPackageBlockSize)*onePkgSize - encOffset

	// Check for the remainder, to figure if we need one extract package to read from.
	if (offset+length)%sseDAREPackageBlockSize > 0 {
		encLength += onePkgSize
	}

	if encLength+encOffset > objInfo.EncryptedSize() {
		encLength = objInfo.EncryptedSize() - encOffset
	}
	return seqNumber, encOffset, encLength
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

	sseDAREEncPackageBlockSize := int64(sseDAREPackageBlockSize + sseDAREPackageMetaSize)
	startPkgNum := (off - cumulativeSum) / sseDAREPackageBlockSize

	// Now we can calculate the number of bytes to skip
	skipLen = (off - cumulativeSum) % sseDAREPackageBlockSize

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
	endPkgNum := (endOffset - cumulativeSum) / sseDAREPackageBlockSize
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
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
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
func DecryptCopyObjectInfo(info *ObjectInfo, headers http.Header) (apiErr APIErrorCode, encrypted bool) {
	// Directories are never encrypted.
	if info.IsDir {
		return ErrNone, false
	}
	if apiErr, encrypted = ErrNone, crypto.IsEncrypted(info.UserDefined); !encrypted && crypto.SSECopy.IsRequested(headers) {
		apiErr = ErrInvalidEncryptionParameters
	} else if encrypted {
		if (!crypto.SSECopy.IsRequested(headers) && crypto.SSEC.IsEncrypted(info.UserDefined)) ||
			(crypto.SSECopy.IsRequested(headers) && crypto.S3.IsEncrypted(info.UserDefined)) {
			apiErr = ErrSSEEncryptedObject
			return
		}
		var err error
		if info.Size, err = info.DecryptedSize(); err != nil {
			apiErr = toAPIErrorCode(context.Background(), err)
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
