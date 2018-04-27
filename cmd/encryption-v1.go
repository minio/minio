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
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/minio/minio/cmd/logger"
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

const (
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

const (
	// SSECustomerKeySize is the size of valid client provided encryption keys in bytes.
	// Currently AWS supports only AES256. So the SSE-C key size is fixed to 32 bytes.
	SSECustomerKeySize = 32

	// SSEIVSize is the size of the IV data
	SSEIVSize = 32 // 32 bytes

	// SSECustomerAlgorithmAES256 the only valid S3 SSE-C encryption algorithm identifier.
	SSECustomerAlgorithmAES256 = "AES256"

	// SSE dare package block size.
	sseDAREPackageBlockSize = 64 * 1024 // 64KiB bytes

	// SSE dare package meta padding bytes.
	sseDAREPackageMetaSize = 32 // 32 bytes

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

// SSESealAlgorithmDareSha256 specifies DARE as authenticated en/decryption scheme and SHA256 as cryptographic
// hash function.
const SSESealAlgorithmDareSha256 = "DARE-SHA256"

// hasSSECustomerHeader returns true if the given HTTP header
// contains server-side-encryption with customer provided key fields.
func hasSSECustomerHeader(header http.Header) bool {
	return header.Get(SSECustomerAlgorithm) != "" || header.Get(SSECustomerKey) != "" || header.Get(SSECustomerKeyMD5) != ""
}

// hasSSECopyCustomerHeader returns true if the given HTTP header
// contains copy source server-side-encryption with customer provided key fields.
func hasSSECopyCustomerHeader(header http.Header) bool {
	return header.Get(SSECopyCustomerAlgorithm) != "" || header.Get(SSECopyCustomerKey) != "" || header.Get(SSECopyCustomerKeyMD5) != ""
}

// ParseSSECopyCustomerRequest parses the SSE-C header fields of the provided request.
// It returns the client provided key on success.
func ParseSSECopyCustomerRequest(r *http.Request) (key []byte, err error) {
	if !globalIsSSL { // minio only supports HTTP or HTTPS requests not both at the same time
		// we cannot use r.TLS == nil here because Go's http implementation reflects on
		// the net.Conn and sets the TLS field of http.Request only if it's an tls.Conn.
		// Minio uses a BufConn (wrapping a tls.Conn) so the type check within the http package
		// will always fail -> r.TLS is always nil even for TLS requests.
		return nil, errInsecureSSERequest
	}
	header := r.Header
	if algorithm := header.Get(SSECopyCustomerAlgorithm); algorithm != SSECustomerAlgorithmAES256 {
		return nil, errInvalidSSEAlgorithm
	}
	if header.Get(SSECopyCustomerKey) == "" {
		return nil, errMissingSSEKey
	}
	if header.Get(SSECopyCustomerKeyMD5) == "" {
		return nil, errMissingSSEKeyMD5
	}

	key, err = base64.StdEncoding.DecodeString(header.Get(SSECopyCustomerKey))
	if err != nil {
		return nil, errInvalidSSEKey
	}

	if len(key) != SSECustomerKeySize {
		return nil, errInvalidSSEKey
	}
	// Make sure we purged the keys from http headers by now.
	header.Del(SSECopyCustomerKey)

	keyMD5, err := base64.StdEncoding.DecodeString(header.Get(SSECopyCustomerKeyMD5))
	if err != nil {
		return nil, errSSEKeyMD5Mismatch
	}
	if md5Sum := md5.Sum(key); !bytes.Equal(md5Sum[:], keyMD5) {
		return nil, errSSEKeyMD5Mismatch
	}
	return key, nil
}

// ParseSSECustomerRequest parses the SSE-C header fields of the provided request.
// It returns the client provided key on success.
func ParseSSECustomerRequest(r *http.Request) (key []byte, err error) {
	return ParseSSECustomerHeader(r.Header)
}

// ParseSSECustomerHeader parses the SSE-C header fields and returns
// the client provided key on success.
func ParseSSECustomerHeader(header http.Header) (key []byte, err error) {
	if !globalIsSSL { // minio only supports HTTP or HTTPS requests not both at the same time
		// we cannot use r.TLS == nil here because Go's http implementation reflects on
		// the net.Conn and sets the TLS field of http.Request only if it's an tls.Conn.
		// Minio uses a BufConn (wrapping a tls.Conn) so the type check within the http package
		// will always fail -> r.TLS is always nil even for TLS requests.
		return nil, errInsecureSSERequest
	}
	if algorithm := header.Get(SSECustomerAlgorithm); algorithm != SSECustomerAlgorithmAES256 {
		return nil, errInvalidSSEAlgorithm
	}
	if header.Get(SSECustomerKey) == "" {
		return nil, errMissingSSEKey
	}
	if header.Get(SSECustomerKeyMD5) == "" {
		return nil, errMissingSSEKeyMD5
	}

	key, err = base64.StdEncoding.DecodeString(header.Get(SSECustomerKey))
	if err != nil {
		return nil, errInvalidSSEKey
	}

	if len(key) != SSECustomerKeySize {
		return nil, errInvalidSSEKey
	}
	// Make sure we purged the keys from http headers by now.
	header.Del(SSECustomerKey)

	keyMD5, err := base64.StdEncoding.DecodeString(header.Get(SSECustomerKeyMD5))
	if err != nil {
		return nil, errSSEKeyMD5Mismatch
	}
	if md5Sum := md5.Sum(key); !bytes.Equal(md5Sum[:], keyMD5) {
		return nil, errSSEKeyMD5Mismatch
	}
	return key, nil
}

// This function rotates old to new key.
func rotateKey(oldKey []byte, newKey []byte, metadata map[string]string) error {
	delete(metadata, SSECustomerKey) // make sure we do not save the key by accident

	if metadata[ServerSideEncryptionSealAlgorithm] != SSESealAlgorithmDareSha256 { // currently DARE-SHA256 is the only option
		return errObjectTampered
	}
	iv, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionIV])
	if err != nil || len(iv) != SSEIVSize {
		return errObjectTampered
	}
	sealedKey, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionSealedKey])
	if err != nil || len(sealedKey) != 64 {
		return errObjectTampered
	}

	sha := sha256.New() // derive key encryption key
	sha.Write(oldKey)
	sha.Write(iv)
	keyEncryptionKey := sha.Sum(nil)

	objectEncryptionKey := bytes.NewBuffer(nil) // decrypt object encryption key
	n, err := sio.Decrypt(objectEncryptionKey, bytes.NewReader(sealedKey), sio.Config{
		Key: keyEncryptionKey,
	})
	if n != 32 || err != nil { // Either the provided key does not match or the object was tampered.
		if subtle.ConstantTimeCompare(oldKey, newKey) == 1 {
			return errInvalidSSEParameters // AWS returns special error for equal but invalid keys.
		}
		return errSSEKeyMismatch // To provide strict AWS S3 compatibility we return: access denied.
	}
	if subtle.ConstantTimeCompare(oldKey, newKey) == 1 {
		return nil // we don't need to rotate keys if newKey == oldKey
	}

	nonce := make([]byte, 32) // generate random values for key derivation
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}

	niv := sha256.Sum256(nonce[:]) // derive key encryption key
	sha = sha256.New()
	sha.Write(newKey)
	sha.Write(niv[:])
	keyEncryptionKey = sha.Sum(nil)

	sealedKeyW := bytes.NewBuffer(nil) // sealedKey := 16 byte header + 32 byte payload + 16 byte tag
	n, err = sio.Encrypt(sealedKeyW, bytes.NewReader(objectEncryptionKey.Bytes()), sio.Config{
		Key: keyEncryptionKey,
	})
	if n != 64 || err != nil {
		return errors.New("failed to seal object encryption key") // if this happens there's a bug in the code (may panic ?)
	}

	metadata[ServerSideEncryptionIV] = base64.StdEncoding.EncodeToString(niv[:])
	metadata[ServerSideEncryptionSealAlgorithm] = SSESealAlgorithmDareSha256
	metadata[ServerSideEncryptionSealedKey] = base64.StdEncoding.EncodeToString(sealedKeyW.Bytes())
	return nil
}

func newEncryptMetadata(key []byte, metadata map[string]string) ([]byte, error) {
	delete(metadata, SSECustomerKey) // make sure we do not save the key by accident

	// security notice:
	//  - If the first 32 bytes of the random value are ever repeated under the same client-provided
	//    key the encrypted object will not be tamper-proof. [ P(coll) ~= 1 / 2^(256 / 2)]
	//  - If the last 32 bytes of the random value are ever repeated under the same client-provided
	//    key an adversary may be able to extract the object encryption key. This depends on the
	//    authenticated en/decryption scheme. The DARE format will generate an 8 byte nonce which must
	//    be repeated in addition to reveal the object encryption key.
	//    [ P(coll) ~= 1 / 2^((256 + 64) / 2) ]
	nonce := make([]byte, 32+SSEIVSize) // generate random values for key derivation
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	sha := sha256.New() // derive object encryption key
	sha.Write(key)
	sha.Write(nonce[:32])
	objectEncryptionKey := sha.Sum(nil)

	iv := sha256.Sum256(nonce[32:]) // derive key encryption key
	sha = sha256.New()
	sha.Write(key)
	sha.Write(iv[:])
	keyEncryptionKey := sha.Sum(nil)

	sealedKey := bytes.NewBuffer(nil) // sealedKey := 16 byte header + 32 byte payload + 16 byte tag
	n, err := sio.Encrypt(sealedKey, bytes.NewReader(objectEncryptionKey), sio.Config{
		Key: keyEncryptionKey,
	})
	if n != 64 || err != nil {
		return nil, errors.New("failed to seal object encryption key") // if this happens there's a bug in the code (may panic ?)
	}

	metadata[ServerSideEncryptionIV] = base64.StdEncoding.EncodeToString(iv[:])
	metadata[ServerSideEncryptionSealAlgorithm] = SSESealAlgorithmDareSha256
	metadata[ServerSideEncryptionSealedKey] = base64.StdEncoding.EncodeToString(sealedKey.Bytes())

	return objectEncryptionKey, nil
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
	key, err := ParseSSECustomerRequest(r)
	if err != nil {
		return nil, err
	}
	return newEncryptReader(content, key, metadata)
}

// DecryptCopyRequest decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptCopyRequest(client io.Writer, r *http.Request, metadata map[string]string) (io.WriteCloser, error) {
	key, err := ParseSSECopyCustomerRequest(r)
	if err != nil {
		return nil, err
	}
	delete(metadata, SSECopyCustomerKey) // make sure we do not save the key by accident
	return newDecryptWriter(client, key, 0, metadata)
}

func decryptObjectInfo(key []byte, metadata map[string]string) ([]byte, error) {
	if metadata[ServerSideEncryptionSealAlgorithm] != SSESealAlgorithmDareSha256 { // currently DARE-SHA256 is the only option
		return nil, errObjectTampered
	}
	iv, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionIV])
	if err != nil || len(iv) != SSEIVSize {
		return nil, errObjectTampered
	}
	sealedKey, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionSealedKey])
	if err != nil || len(sealedKey) != 64 {
		return nil, errObjectTampered
	}

	sha := sha256.New() // derive key encryption key
	sha.Write(key)
	sha.Write(iv)
	keyEncryptionKey := sha.Sum(nil)

	objectEncryptionKey := bytes.NewBuffer(nil) // decrypt object encryption key
	n, err := sio.Decrypt(objectEncryptionKey, bytes.NewReader(sealedKey), sio.Config{
		Key: keyEncryptionKey,
	})
	if n != 32 || err != nil {
		// Either the provided key does not match or the object was tampered.
		// To provide strict AWS S3 compatibility we return: access denied.
		return nil, errSSEKeyMismatch
	}
	return objectEncryptionKey.Bytes(), nil
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
	key, err := ParseSSECustomerRequest(r)
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
		key, err = ParseSSECopyCustomerRequest(w.req)
	} else {
		w.req.Header.Set(SSECustomerKey, w.customerKeyHeader)
		key, err = ParseSSECustomerRequest(w.req)
	}
	if err != nil {
		return err
	}

	objectEncryptionKey, err := decryptObjectInfo(key, m)
	if err != nil {
		return err
	}

	var partIDbin [4]byte
	binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

	mac := hmac.New(sha256.New, objectEncryptionKey) // derive part encryption key from part ID and object key
	mac.Write(partIDbin[:])
	partEncryptionKey := mac.Sum(nil)

	// make sure we do not save the key by accident
	if w.copySource {
		delete(m, SSECopyCustomerKey)
	} else {
		delete(m, SSECustomerKey)
	}

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

	startSeqNum := partStartOffset / sseDAREPackageBlockSize
	partEncRelOffset := int64(startSeqNum) * (sseDAREPackageBlockSize + sseDAREPackageMetaSize)

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

	return seqNumber, encOffset, encLength
}

// IsEncryptedMultipart - is the encrypted content multiparted?
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
// It returns an error if the object is not encrypted or marked as encrypted
// but has an invalid size.
func (o *ObjectInfo) DecryptedSize() (int64, error) {
	if !o.IsEncrypted() {
		return 0, errors.New("Cannot compute decrypted size of an unencrypted object")
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
	if apiErr, encrypted = ErrNone, info.IsEncrypted(); !encrypted && hasSSECopyCustomerHeader(headers) {
		apiErr = ErrInvalidEncryptionParameters
	} else if encrypted {
		if !hasSSECopyCustomerHeader(headers) {
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

// DecryptObjectInfo tries to decrypt the provided object if it is encrypted.
// It fails if the object is encrypted and the HTTP headers don't contain
// SSE-C headers or the object is not encrypted but SSE-C headers are provided. (AWS behavior)
// DecryptObjectInfo returns 'ErrNone' if the object is not encrypted or the
// decryption succeeded.
//
// DecryptObjectInfo also returns whether the object is encrypted or not.
func DecryptObjectInfo(info *ObjectInfo, headers http.Header) (apiErr APIErrorCode, encrypted bool) {
	// Directories are never encrypted.
	if info.IsDir {
		return ErrNone, false
	}
	if apiErr, encrypted = ErrNone, info.IsEncrypted(); !encrypted && hasSSECustomerHeader(headers) {
		apiErr = ErrInvalidEncryptionParameters
	} else if encrypted {
		if !hasSSECustomerHeader(headers) {
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
