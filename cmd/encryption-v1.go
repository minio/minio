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
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/minio/minio/pkg/ioutil"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

var (
	// AWS errors for invalid SSE-C requests.
	errInsecureSSERequest   = errors.New("SSE-C requests require TLS connections")
	errEncryptedObject      = errors.New("The object was stored using a form of SSE")
	errInvalidSSEAlgorithm  = errors.New("The SSE-C algorithm is not valid")
	errMissingSSEKey        = errors.New("The SSE-C request is missing the customer key")
	errInvalidSSEKey        = errors.New("The SSE-C key is invalid")
	errMissingSSEKeyMD5     = errors.New("The SSE-C request is missing the customer key MD5")
	errSSEKeyMD5Mismatch    = errors.New("The key MD5 does not match the SSE-C key")
	errSSEKeyMismatch       = errors.New("The SSE-C key is not correct")                  // access denied
	errInvalidSSEParameters = errors.New("The SSE-C key for key-rotation is not correct") // special access denied

	// Additional Minio errors for SSE-C requests.
	errObjectTampered = errors.New("The requested object was modified and may be compromised")
)

const ( // part of AWS S3 specification
	// SSECustomerAlgorithm is the AWS SSE-C algorithm HTTP header key.
	SSECustomerAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"
	// SSECustomerKey is the AWS SSE-C encryption key HTTP header key.
	SSECustomerKey = "X-Amz-Server-Side-Encryption-Customer-Key"
	// SSECustomerKeyMD5 is the AWS SSE-C encryption key MD5 HTTP header key.
	SSECustomerKeyMD5 = "X-Amz-Server-Side-Encryption-Customer-Key-MD5"

	// SSECopyCustomerAlgorithm is the AWS SSE-C algorithm HTTP header key for CopyObject API.
	SSECopyCustomerAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"
	// SSECopyCustomerKey is the AWS SSE-C encryption key HTTP header key for CopyObject API.
	SSECopyCustomerKey = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
	// SSECopyCustomerKeyMD5 is the AWS SSE-C encryption key MD5 HTTP header key for CopyObject API.
	SSECopyCustomerKeyMD5 = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-MD5"
)

const ( // part of AWS S3 specification
	// SSECustomerKeySize is the size of valid client provided encryption keys in bytes.
	// Currently AWS supports only AES256. So the SSE-C key size is fixed to 32 bytes.
	SSECustomerKeySize = 32

	// SSECustomerAlgorithmAES256 the only valid S3 SSE-C encryption algorithm identifier.
	SSECustomerAlgorithmAES256 = "AES256"
)

// SSE-C key derivation, key verification and key update:
// H: Hash function [32 = |H(m)|]
// AE: authenticated encryption scheme, AD: authenticated decryption scheme [m = AD(k, AE(k, m))]
//
// Key derivation:
// 	 Input:
// 		key     := 32 bytes              # client provided key
// 		Re, Rm  := 32 bytes, 32 bytes    # uniformly random
//
//   Seal:
// 		k       := H(key || Re)          # object encryption key
// 		r       := H(Rm)                 # save as object metadata [ServerSideEncryptionIV]
//		KeK     := H(key || r)           # key encryption key
// 		K       := AE(KeK, k)            # save as object metadata [ServerSideEncryptionSealedKey]
// ------------------------------------------------------------------------------------------------
// Key verification:
// 	 Input:
// 		key     := 32 bytes              # client provided key
// 		r       := 32 bytes              # object metadata [ServerSideEncryptionIV]
// 		K       := 32 bytes              # object metadata [ServerSideEncryptionSealedKey]
//
//   Open:
//		KeK     := H(key || r)           # key encryption key
//		k       := AD(Kek, K)            # object encryption key
// -------------------------------------------------------------------------------------------------
// Key update:
// 	 Input:
// 		key     := 32 bytes              # old client provided key
// 		key'    := 32 bytes              # new client provided key
// 		Rm      := 32 bytes              # uniformly random
// 		r       := 32 bytes              # object metadata [ServerSideEncryptionIV]
// 		K       := 32 bytes              # object metadata [ServerSideEncryptionSealedKey]
//
//   Update:
//      1. open:
//			KeK     := H(key || r)       # key encryption key
//			k       := AD(Kek, K)        # object encryption key
//      2. seal:
//			r'      := H(Rm)			 # save as object metadata [ServerSideEncryptionIV]
//			KeK'    := H(key' || r')	 # new key encryption key
// 			K'      := AE(KeK', k)       # save as object metadata [ServerSideEncryptionSealedKey]
//
//
// For SSE-C multipart each part is encrypted using a separate part-key derived from the
// object-encryption-key and the part-number:
// KDF: key-derivation-function [32 = |KDF(K, x)|]
//
// Part-key derivation:
//	 Input:
// 		k       := 32 bytes              # object encrypted key (see above)
// 		i       := integer               # part ID (1<= i <= 10000)
//
//	 Part-Key derivation:
//	 	bin_ID := little_endian_encode(i) # 4 byte little endian representation of i
//      kp     := KDF(k, bin_ID)
//
//	 Output:
//	 	kp

const (
	// ServerSideEncryptionIV is a 32 byte randomly generated IV used to derive an
	// unique key encryption key from the client provided key. The combination of this value
	// and the client-provided key MUST be unique.
	ServerSideEncryptionIV = ReservedMetadataPrefix + "Server-Side-Encryption-Iv"

	// ServerSideEncryptionSealAlgorithm identifies a combination of a cryptographic hash function and
	// an authenticated en/decryption scheme to seal the object encryption key.
	ServerSideEncryptionSealAlgorithm = ReservedMetadataPrefix + "Server-Side-Encryption-Seal-Algorithm"

	// ServerSideEncryptionSealedKey is the sealed object encryption key. The sealed key can be decrypted
	// by the key encryption key derived from the client provided key and the server-side-encryption IV.
	ServerSideEncryptionSealedKey = ReservedMetadataPrefix + "Server-Side-Encryption-Sealed-Key"
)

const (
	// SSEIVSize is the size of the randomly chosen IV used to derive the key-encryption-key.
	SSEIVSize = 32

	// DAREPayloadSize is the max. size of the package payload for a DARE 2.0 package.
	DAREPayloadSize = 1 << 16

	// DAREOverhead is the overhead (header + tag) added by encrypting one payload using DARE 2.0.
	DAREOverhead = 32

	// SSESealAlgorithmDareSha256 specifies DARE as authenticated en/decryption scheme and SHA256 as
	// cryptographic hash function.
	SSESealAlgorithmDareSha256 = "DARE-SHA256"
)

var (
	// SSECustomerHeaders can parse regular SSE-C requests.
	SSECustomerHeaders = sseCustomerHeaders{
		algorithm: SSECustomerAlgorithm,
		key:       SSECustomerKey,
		keyMD5:    SSECustomerKeyMD5,
	}

	// SSECopyCustomerHeaders can parse SSE-C copy requests.
	SSECopyCustomerHeaders = sseCustomerHeaders{
		algorithm: SSECopyCustomerAlgorithm,
		key:       SSECopyCustomerKey,
		keyMD5:    SSECopyCustomerKeyMD5,
	}
)

// sseCustomerHeaders contains the SSE-C headers
// which must be present in the client request.
type sseCustomerHeaders struct {
	algorithm string
	key       string
	keyMD5    string
}

// Contains returns true if the provided headers contain
// the at least one SSE-C header field.
func (sse sseCustomerHeaders) Contains(header http.Header) bool {
	return header.Get(sse.algorithm) != "" || header.Get(sse.key) != "" || header.Get(sse.keyMD5) != ""
}

// Parse parses the SSE-C header fields of the provided HTTP headers.
// It returns the client provided 256 bit key on success.
func (sse sseCustomerHeaders) Parse(header http.Header) (key []byte, err error) {
	defer func() { header.Del(sse.algorithm) }() // Remove SSE-C key from header

	if !globalIsSSL { // minio only supports HTTP or HTTPS requests not both at the same time
		// we cannot use r.TLS == nil here because Go's http implementation reflects on
		// the net.Conn and sets the TLS field of http.Request only if it's an tls.Conn.
		// Minio uses a BufConn (wrapping a tls.Conn) so the type check within the http package
		// will always fail -> r.TLS is always nil even for TLS requests.
		return nil, errInsecureSSERequest
	}
	if algorithm := header.Get(sse.algorithm); algorithm != SSECustomerAlgorithmAES256 {
		return nil, errInvalidSSEAlgorithm
	}
	if header.Get(sse.key) == "" {
		return nil, errMissingSSEKey
	}
	if header.Get(sse.keyMD5) == "" {
		return nil, errMissingSSEKeyMD5
	}

	key, err = base64.StdEncoding.DecodeString(header.Get(sse.key))
	if err != nil || len(key) != SSECustomerKeySize {
		return nil, errInvalidSSEKey
	}

	keyMD5, err := base64.StdEncoding.DecodeString(header.Get(sse.keyMD5))
	if err != nil {
		return nil, errSSEKeyMD5Mismatch
	}
	if md5Sum := md5.Sum(key); !bytes.Equal(md5Sum[:], keyMD5) {
		return nil, errSSEKeyMD5Mismatch
	}
	return key, nil
}

// ObjectEncryptionKey is a 256 bit key used to either en/decrypt a single-part object
// or derive an unique key per object part for multi-part objects.
type ObjectEncryptionKey [32]byte

// DeriveObjectEncryptionKey derives an unique object-encryption-key from the client-provided
// key. It returns a error if the client-provided key is not 256 bits long.
func DeriveObjectEncryptionKey(clientKey []byte) (key ObjectEncryptionKey, err error) {
	if len(clientKey) != SSECustomerKeySize {
		return key, errInvalidSSEKey
	}
	var random [32]byte
	if _, err = io.ReadFull(rand.Reader, random[:]); err != nil {
		return key, fmt.Errorf("Failed to read random value from PRNG: %v", err)
	}
	sha := sha256.New()
	sha.Write(clientKey)
	sha.Write(random[:])
	sha.Sum(key[:0]) // Sum appends the hash (32 bytes) to the provided slice
	return
}

// DerivePartKey derives an unique key from an objecct-encryption-key and the provided part ID.
func (k ObjectEncryptionKey) DerivePartKey(partID int) (key [32]byte) {
	var binPartID [4]byte
	binary.LittleEndian.PutUint32(binPartID[:], uint32(partID))

	mac := hmac.New(sha256.New, k[:])
	mac.Write(binPartID[:])
	mac.Sum(key[:0]) // Sum appends the hash (32 bytes) to the provided slice
	return
}

// KeyEncryptionKey is a 256 bit key and a 256 bit IV used to seal/open an
// object-encryption-key. The key-encryption-key is the key which is changed
// during an S3 key rotatation request.
type KeyEncryptionKey struct {
	key, iv [32]byte
}

func deriveKeyEncryptionKey(clientKey, iv []byte) (key KeyEncryptionKey, err error) {
	if len(clientKey) != SSECustomerKeySize {
		return key, errInvalidSSEKey
	}
	if len(iv) != SSEIVSize {
		return key, errors.New("invalid IV: IV must be 256 bits long")
	}
	copy(key.iv[:], iv)
	sha := sha256.New()
	sha.Write(clientKey)
	sha.Write(key.iv[:])
	sha.Sum(key.key[:0]) // Sum appends the hash (32 bytes) to the provided slice
	return
}

// DeriveKeyEncryptionKey derives an unique key-encryption-key from the client-provided
// key. It returns an error if the client-provided key is not 256 bits long.
func DeriveKeyEncryptionKey(clientKey []byte) (KeyEncryptionKey, error) {
	if len(clientKey) != SSECustomerKeySize {
		return KeyEncryptionKey{}, errInvalidSSEKey
	}
	var random [32]byte
	if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
		return KeyEncryptionKey{}, fmt.Errorf("Failed to read random value from PRNG: %v", err)
	}
	iv := sha256.Sum256(random[:])
	return deriveKeyEncryptionKey(clientKey, iv[:])
}

// UnmarshalObjectEncryptionKey parses and verifies the provided metdata. It returns the decrypted
// object-encrypted-key using the client-provided key on success.
func UnmarshalObjectEncryptionKey(clientKey []byte, metadata map[string]string) (key ObjectEncryptionKey, err error) {
	if len(clientKey) != SSECustomerKeySize {
		return key, errInvalidSSEKey
	}
	if metadata[ServerSideEncryptionSealAlgorithm] != SSESealAlgorithmDareSha256 {
		return key, errObjectTampered // currently DARE-SHA256 is the only option
	}

	iv, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionIV])
	if err != nil {
		return key, errObjectTampered
	}
	kek, err := deriveKeyEncryptionKey(clientKey, iv)
	if err != nil {
		return key, errObjectTampered
	}

	sealedKey, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionSealedKey])
	if err != nil {
		return key, errObjectTampered
	}
	return kek.OpenKey(sealedKey)
}

// Marshal writes the IV and the object-encryption-key to the provided metadata map.
// Marshal always seals the provided object-encryption-key before storing it.
func (k KeyEncryptionKey) Marshal(key ObjectEncryptionKey, metadata map[string]string) {
	delete(metadata, SSECustomerKey)
	delete(metadata, SSECopyCustomerKey)
	metadata[ServerSideEncryptionIV] = base64.StdEncoding.EncodeToString(k.iv[:])
	metadata[ServerSideEncryptionSealAlgorithm] = SSESealAlgorithmDareSha256
	metadata[ServerSideEncryptionSealedKey] = base64.StdEncoding.EncodeToString(k.SealKey(key))
}

// SealKey encryts and returns the provided object-encryption-key.
func (k KeyEncryptionKey) SealKey(key ObjectEncryptionKey) []byte {
	sealedKey := bytes.NewBuffer(nil)
	n, err := sio.Encrypt(sealedKey, bytes.NewReader(key[:]), sio.Config{Key: k.key[:]})
	if n != 64 || err != nil {
		// fail hard - otherwise we may not detect that we store only partial object key.
		panic("failed to seal object encryption key") // Indicates that there's a bug in the sio package.
	}
	return sealedKey.Bytes()
}

// OpenKey tries to decryt the provided sealed key. It returns the object-encryption-key
// on success.
func (k KeyEncryptionKey) OpenKey(sealedKey []byte) (key ObjectEncryptionKey, err error) {
	if len(sealedKey) != 32+DAREOverhead {
		return key, errObjectTampered
	}
	openedKey := bytes.NewBuffer(nil)
	n, err := sio.Decrypt(openedKey, bytes.NewReader(sealedKey), sio.Config{Key: k.key[:]})
	if n != 32 || err != nil {
		// Either the provided key does not match or the object was tampered.
		// To provide strict AWS S3 compatibility we return: access denied.
		return key, errSSEKeyMismatch
	}
	copy(key[:], openedKey.Bytes())
	return
}

// RotateKeys rotates the key-encryption-key. It extracts the object-encrypted-key
// from the provided metadata using the old key and stores the sealed object-encryption-key
// using the new key.
//
// RotateKeys only verifies whether the provided old key is correct if the old and the new key
// are equal.
func RotateKeys(oldKey, newKey []byte, metadata map[string]string) error {
	if len(oldKey) != SSECustomerKeySize || len(newKey) != SSECustomerKeySize {
		return errInvalidSSEKey
	}
	objectKey, err := UnmarshalObjectEncryptionKey(oldKey, metadata) // always check that client sends correct key - even if both keys are equal
	if err != nil {
		if subtle.ConstantTimeCompare(oldKey, newKey) == 1 { // we do not rotate keys if the client keys are equal
			return errInvalidSSEParameters // return special error if old key is invalid and old == new (AWS S3 behavior)
		}
		return err
	}
	if subtle.ConstantTimeCompare(oldKey, newKey) == 1 { // we do not rotate keys if the client keys are equal
		return nil
	}
	kek, err := DeriveKeyEncryptionKey(newKey)
	if err != nil {
		return err
	}
	kek.Marshal(objectKey, metadata)
	return nil
}

func newEncryptMetadata(key []byte, metadata map[string]string) ([]byte, error) {
	kek, err := DeriveKeyEncryptionKey(key)
	if err != nil {
		return nil, err
	}
	objectKey, err := DeriveObjectEncryptionKey(key)
	if err != nil {
		return nil, err
	}
	kek.Marshal(objectKey, metadata)
	return objectKey[:], nil
}

func newEncryptReader(content io.Reader, key []byte, metadata map[string]string) (io.Reader, error) {
	objectEncryptionKey, err := newEncryptMetadata(key, metadata)
	if err != nil {
		return nil, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey})
	if err != nil {
		return nil, errInvalidSSEKey
	}
	return reader, nil
}

// EncryptRequest takes the client provided content and encrypts the data
// with the client provided key. It also marks the object as client-side-encrypted
// and sets the correct headers.
func EncryptRequest(content io.Reader, r *http.Request, metadata map[string]string) (io.Reader, error) {
	key, err := SSECustomerHeaders.Parse(r.Header)
	if err != nil {
		return nil, err
	}
	return newEncryptReader(content, key, metadata)
}

// DecryptCopyRequest decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptCopyRequest(client io.Writer, r *http.Request, metadata map[string]string) (io.WriteCloser, error) {
	key, err := SSECopyCustomerHeaders.Parse(r.Header)
	if err != nil {
		return nil, err
	}
	delete(metadata, SSECopyCustomerKey) // make sure we do not save the key by accident
	return newDecryptWriter(client, key, 0, metadata)
}

func decryptObjectInfo(key []byte, metadata map[string]string) ([]byte, error) {
	objectKey, err := UnmarshalObjectEncryptionKey(key, metadata)
	if err != nil {
		return nil, err
	}
	delete(metadata, ServerSideEncryptionIV)
	delete(metadata, ServerSideEncryptionSealAlgorithm)
	delete(metadata, ServerSideEncryptionSealedKey)
	return objectKey[:], err
}

func newDecryptWriter(client io.Writer, key []byte, seqNumber uint32, metadata map[string]string) (io.WriteCloser, error) {
	objectEncryptionKey, err := decryptObjectInfo(key, metadata)
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
		return nil, errInvalidSSEKey
	}

	delete(metadata, ServerSideEncryptionIV)
	delete(metadata, ServerSideEncryptionSealAlgorithm)
	delete(metadata, ServerSideEncryptionSealedKey)
	delete(metadata, ReservedMetadataPrefix+"Encrypted-Multipart")
	return writer, nil
}

// DecryptRequestWithSequenceNumber decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptRequestWithSequenceNumber(client io.Writer, r *http.Request, seqNumber uint32, metadata map[string]string) (io.WriteCloser, error) {
	key, err := SSECustomerHeaders.Parse(r.Header)
	if err != nil {
		return nil, err
	}
	delete(metadata, SSECustomerKey) // make sure we do not save the key by accident
	return newDecryptWriter(client, key, seqNumber, metadata)
}

// DecryptRequest decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptRequest(client io.Writer, r *http.Request, metadata map[string]string) (io.WriteCloser, error) {
	return DecryptRequestWithSequenceNumber(client, r, 0, metadata)
}

// DecryptBlocksWriter - decrypts multipart parts, while implementing a io.Writer compatible interface.
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
	parts    []objectPartInfo
	req      *http.Request
	metadata map[string]string

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
		w.req.Header.Set(SSECopyCustomerKey, w.customerKeyHeader)
		key, err = SSECopyCustomerHeaders.Parse(w.req.Header)
	} else {
		w.req.Header.Set(SSECustomerKey, w.customerKeyHeader)
		key, err = SSECustomerHeaders.Parse(w.req.Header)
	}
	if err != nil {
		return err
	}

	objectEncryptionKey, err := UnmarshalObjectEncryptionKey(key, m)
	if err != nil {
		return err
	}
	partEncryptionKey := objectEncryptionKey.DerivePartKey(partID)

	// make sure to provide a NopCloser such that a Close
	// on sio.decryptWriter doesn't close the underlying writer's
	// close which perhaps can close the stream prematurely.
	decrypter, err := newDecryptWriterWithObjectKey(ioutil.NopCloser(w.writer), partEncryptionKey[:], w.startSeqNum, m)
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
func DecryptAllBlocksCopyRequest(client io.Writer, r *http.Request, objInfo ObjectInfo) (io.WriteCloser, int64, error) {
	w, _, size, err := DecryptBlocksRequest(client, r, 0, objInfo.Size, objInfo, true)
	return w, size, err
}

// DecryptBlocksRequest - setup a struct which can decrypt many concatenated encrypted data
// parts information helps to know the boundaries of each encrypted data block.
func DecryptBlocksRequest(client io.Writer, r *http.Request, startOffset, length int64, objInfo ObjectInfo, copySource bool) (io.WriteCloser, int64, int64, error) {
	seqNumber, encStartOffset, encLength := getEncryptedStartOffset(startOffset, length)

	// Encryption length cannot be bigger than the file size, if it is
	// which is allowed in AWS S3, we simply default to EncryptedSize().
	if encLength+encStartOffset > objInfo.EncryptedSize() {
		encLength = objInfo.EncryptedSize() - encStartOffset
	}

	if len(objInfo.Parts) == 0 || !objInfo.IsEncryptedMultipart() {
		var writer io.WriteCloser
		var err error
		if copySource {
			writer, err = DecryptCopyRequest(client, r, objInfo.UserDefined)
		} else {
			writer, err = DecryptRequestWithSequenceNumber(client, r, seqNumber, objInfo.UserDefined)
		}
		if err != nil {
			return nil, 0, 0, err
		}
		return writer, encStartOffset, encLength, nil
	}

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

	startSeqNum := partStartOffset / DAREPayloadSize
	partEncRelOffset := int64(startSeqNum) * (DAREPayloadSize + DAREOverhead)

	w := &DecryptBlocksWriter{
		writer:            client,
		startSeqNum:       uint32(startSeqNum),
		partEncRelOffset:  partEncRelOffset,
		parts:             objInfo.Parts,
		partIndex:         partStartIndex,
		req:               r,
		customerKeyHeader: r.Header.Get(SSECustomerKey),
		copySource:        copySource,
	}

	w.metadata = map[string]string{}
	// Copy encryption metadata for internal use.
	for k, v := range objInfo.UserDefined {
		w.metadata[k] = v
	}

	// Purge all the encryption headers.
	delete(objInfo.UserDefined, ServerSideEncryptionIV)
	delete(objInfo.UserDefined, ServerSideEncryptionSealAlgorithm)
	delete(objInfo.UserDefined, ServerSideEncryptionSealedKey)
	delete(objInfo.UserDefined, ReservedMetadataPrefix+"Encrypted-Multipart")

	if w.copySource {
		w.customerKeyHeader = r.Header.Get(SSECopyCustomerKey)
	}

	if err := w.buildDecrypter(partStartIndex + 1); err != nil {
		return nil, 0, 0, err
	}

	return w, encStartOffset, encLength, nil
}

// getEncryptedStartOffset - fetch sequence number, encrypted start offset and encrypted length.
func getEncryptedStartOffset(offset, length int64) (seqNumber uint32, encOffset int64, encLength int64) {
	onePkgSize := int64(DAREPayloadSize + DAREOverhead)

	seqNumber = uint32(offset / DAREPayloadSize)
	encOffset = int64(seqNumber) * onePkgSize
	// The math to compute the encrypted length is always
	// originalLength i.e (offset+length-1) to be divided under
	// 64KiB blocks which is the payload size for each encrypted
	// block. This is then multiplied by final package size which
	// is basically 64KiB + 32. Finally negate the encrypted offset
	// to get the final encrypted length on disk.
	encLength = ((offset+length)/DAREPayloadSize)*onePkgSize - encOffset

	// Check for the remainder, to figure if we need one extract package to read from.
	if (offset+length)%DAREPayloadSize > 0 {
		encLength += onePkgSize
	}

	return seqNumber, encOffset, encLength
}

// IsEncryptedMultipart returns true if the object is encrypted and was upload using multi-part.
func (o *ObjectInfo) IsEncryptedMultipart() bool {
	_, ok := o.UserDefined[ReservedMetadataPrefix+"Encrypted-Multipart"]
	return ok
}

// IsEncrypted returns true if the object is marked as encrypted.
func (o *ObjectInfo) IsEncrypted() bool {
	if _, ok := o.UserDefined[ServerSideEncryptionIV]; ok {
		return true
	}
	if _, ok := o.UserDefined[ServerSideEncryptionSealAlgorithm]; ok {
		return true
	}
	if _, ok := o.UserDefined[ServerSideEncryptionSealedKey]; ok {
		return true
	}
	return false
}

// IsEncrypted returns true if the object is marked as encrypted.
func (li *ListPartsInfo) IsEncrypted() bool {
	if _, ok := li.UserDefined[ServerSideEncryptionIV]; ok {
		return true
	}
	if _, ok := li.UserDefined[ServerSideEncryptionSealAlgorithm]; ok {
		return true
	}
	if _, ok := li.UserDefined[ServerSideEncryptionSealedKey]; ok {
		return true
	}
	return false
}

// DecryptedSize returns the size of the object after decryption in bytes.
// It returns an error if the object is marked as encrypted
// but has an invalid size.
// DecryptedSize panics if the referred object is not encrypted.
func (o *ObjectInfo) DecryptedSize() (int64, error) {
	if !o.IsEncrypted() {
		panic("cannot compute decrypted size of an object which is not encrypted")
	}
	size, err := sio.DecryptedSize(uint64(o.Size))
	if err != nil {
		err = errObjectTampered // assign correct error type
	}
	return int64(size), err
}

// EncryptedSize returns the size of the object after encryption.
// An encrypted object is always larger than a plain object
// except for zero size objects.
func (o *ObjectInfo) EncryptedSize() int64 {
	size, err := sio.EncryptedSize(uint64(o.Size))
	if err != nil {
		panic(err) // Since AWS S3 allows parts to be 5GB at most this cannot happen - sio max. size is 256 TB
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
	return decryptObjectInfoSize(info, headers, SSECopyCustomerHeaders)
}

// DecryptObjectInfo tries to decrypt the provided object if it is encrypted.
// It fails if the object is encrypted and the HTTP headers don't contain
// SSE-C headers or the object is not encrypted but SSE-C headers are provided. (AWS behavior)
// DecryptObjectInfo returns 'ErrNone' if the object is not encrypted or the
// decryption succeeded.
//
// DecryptObjectInfo also returns whether the object is encrypted or not.
func DecryptObjectInfo(info *ObjectInfo, headers http.Header) (apiErr APIErrorCode, encrypted bool) {
	return decryptObjectInfoSize(info, headers, SSECustomerHeaders)
}

func decryptObjectInfoSize(info *ObjectInfo, headers http.Header, sseHeaders sseCustomerHeaders) (apiErr APIErrorCode, encrypted bool) {
	if info.IsDir { // Directories are never encrypted.
		return ErrNone, false
	}
	if apiErr, encrypted = ErrNone, info.IsEncrypted(); !encrypted && sseHeaders.Contains(headers) {
		apiErr = ErrInvalidEncryptionParameters
	} else if encrypted {
		if !sseHeaders.Contains(headers) {
			apiErr = ErrSSEEncryptedObject
			return
		}
		var err error
		if info.Size, err = info.DecryptedSize(); err != nil {
			apiErr = toAPIErrorCode(err)
		}
	}
	return
}
