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
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
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
	errInsecureSSERequest   = errors.New("SSE-C requests require TLS connections")
	errEncryptedObject      = errors.New("The object was stored using a form of SSE")
	errInvalidSSEParameters = errors.New("The SSE-C key for key-rotation is not correct") // special access denied
	errKMSNotConfigured     = errors.New("KMS not configured for a server side encrypted object")
	// Additional Minio errors for SSE-C requests.
	errObjectTampered = errors.New("The requested object was modified and may be compromised")
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

	// SSEDomain specifies the domain for the derived key - in this case the
	// key should be used for SSE-C.
	SSEDomain = "SSE-C"
)

// hasServerSideEncryptionHeader returns true if the given HTTP header
// contains server-side-encryption.
func hasServerSideEncryptionHeader(header http.Header) bool {
	return crypto.S3.IsRequested(header) || crypto.SSEC.IsRequested(header)
}

// ParseSSECopyCustomerRequest parses the SSE-C header fields of the provided request.
// It returns the client provided key on success.
func ParseSSECopyCustomerRequest(r *http.Request, metadata map[string]string) (key []byte, err error) {
	if !globalIsSSL { // minio only supports HTTP or HTTPS requests not both at the same time
		// we cannot use r.TLS == nil here because Go's http implementation reflects on
		// the net.Conn and sets the TLS field of http.Request only if it's an tls.Conn.
		// Minio uses a BufConn (wrapping a tls.Conn) so the type check within the http package
		// will always fail -> r.TLS is always nil even for TLS requests.
		return nil, errInsecureSSERequest
	}
	if crypto.S3.IsEncrypted(metadata) && crypto.SSECopy.IsRequested(r.Header) {
		return nil, crypto.ErrIncompatibleEncryptionMethod
	}
	k, err := crypto.SSECopy.ParseHTTP(r.Header)
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
	if !globalIsSSL { // minio only supports HTTP or HTTPS requests not both at the same time
		// we cannot use r.TLS == nil here because Go's http implementation reflects on
		// the net.Conn and sets the TLS field of http.Request only if it's an tls.Conn.
		// Minio uses a BufConn (wrapping a tls.Conn) so the type check within the http package
		// will always fail -> r.TLS is always nil even for TLS requests.
		return nil, errInsecureSSERequest
	}
	if crypto.S3.IsRequested(header) && crypto.SSEC.IsRequested(header) {
		return key, crypto.ErrIncompatibleEncryptionMethod
	}

	k, err := crypto.SSEC.ParseHTTP(header)
	return k[:], err
}

// This function rotates old to new key.
func rotateKey(oldKey []byte, newKey []byte, bucket, object string, metadata map[string]string) error {
	delete(metadata, crypto.SSECKey) // make sure we do not save the key by accident

	algorithm := metadata[crypto.SSESealAlgorithm]
	if algorithm != SSESealAlgorithmDareSha256 && algorithm != SSESealAlgorithmDareV2HmacSha256 {
		return errObjectTampered
	}
	iv, err := base64.StdEncoding.DecodeString(metadata[crypto.SSEIV])
	if err != nil || len(iv) != SSEIVSize {
		return errObjectTampered
	}
	sealedKey, err := base64.StdEncoding.DecodeString(metadata[crypto.SSECSealedKey])
	if err != nil || len(sealedKey) != 64 {
		return errObjectTampered
	}

	var (
		minDAREVersion   byte
		keyEncryptionKey [32]byte
	)
	switch algorithm {
	default:
		return errObjectTampered
	case SSESealAlgorithmDareSha256: // legacy key-encryption-key derivation
		minDAREVersion = sio.Version10
		sha := sha256.New()
		sha.Write(oldKey)
		sha.Write(iv)
		sha.Sum(keyEncryptionKey[:0])
	case SSESealAlgorithmDareV2HmacSha256: // key-encryption-key derivation - See: crypto/doc.go
		minDAREVersion = sio.Version20
		mac := hmac.New(sha256.New, oldKey)
		mac.Write(iv)
		mac.Write([]byte(SSEDomain))
		mac.Write([]byte(SSESealAlgorithmDareV2HmacSha256))
		mac.Write([]byte(path.Join(bucket, object)))
		mac.Sum(keyEncryptionKey[:0])
	}

	objectEncryptionKey := bytes.NewBuffer(nil) // decrypt object encryption key
	n, err := sio.Decrypt(objectEncryptionKey, bytes.NewReader(sealedKey), sio.Config{
		MinVersion: minDAREVersion,
		Key:        keyEncryptionKey[:],
	})
	if n != 32 || err != nil { // Either the provided key does not match or the object was tampered.
		if subtle.ConstantTimeCompare(oldKey, newKey) == 1 {
			return errInvalidSSEParameters // AWS returns special error for equal but invalid keys.
		}
		return crypto.ErrInvalidCustomerKey // To provide strict AWS S3 compatibility we return: access denied.
	}
	if subtle.ConstantTimeCompare(oldKey, newKey) == 1 && algorithm != SSESealAlgorithmDareSha256 {
		return nil // we don't need to rotate keys if newKey == oldKey but we may have to upgrade KDF algorithm
	}

	mac := hmac.New(sha256.New, newKey) // key-encryption-key derivation - See: crypto/doc.go
	mac.Write(iv)
	mac.Write([]byte(SSEDomain))
	mac.Write([]byte(SSESealAlgorithmDareV2HmacSha256))
	mac.Write([]byte(path.Join(bucket, object)))
	mac.Sum(keyEncryptionKey[:0])

	sealedKeyW := bytes.NewBuffer(nil) // sealedKey := 16 byte header + 32 byte payload + 16 byte tag
	n, err = sio.Encrypt(sealedKeyW, bytes.NewReader(objectEncryptionKey.Bytes()), sio.Config{
		Key: keyEncryptionKey[:],
	})
	if n != 64 || err != nil {
		return errors.New("failed to seal object encryption key") // if this happens there's a bug in the code (may panic ?)
	}

	metadata[crypto.SSEIV] = base64.StdEncoding.EncodeToString(iv[:])
	metadata[crypto.SSESealAlgorithm] = SSESealAlgorithmDareV2HmacSha256
	metadata[crypto.SSECSealedKey] = base64.StdEncoding.EncodeToString(sealedKeyW.Bytes())
	return nil
}

func newEncryptMetadata(key []byte, bucket, object string, metadata map[string]string, sseS3 bool) ([]byte, error) {
	delete(metadata, crypto.SSECKey) // make sure we do not save the key by accident
	// See crypto/doc.go for detailed description
	nonce := make([]byte, 32+SSEIVSize) // generate random values for key derivation
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	sha := sha256.New() // derive object encryption key
	sha.Write(key)
	sha.Write(nonce[:32])
	objectEncryptionKey := sha.Sum(nil)

	iv := sha256.Sum256(nonce[32:]) // key-encryption-key derivation - See: crypto/doc.go
	mac := hmac.New(sha256.New, key)
	mac.Write(iv[:])
	mac.Write([]byte(SSEDomain))
	mac.Write([]byte(SSESealAlgorithmDareV2HmacSha256))
	mac.Write([]byte(path.Join(bucket, object)))
	keyEncryptionKey := mac.Sum(nil)

	sealedKey := bytes.NewBuffer(nil) // sealedKey := 16 byte header + 32 byte payload + 16 byte tag
	n, err := sio.Encrypt(sealedKey, bytes.NewReader(objectEncryptionKey), sio.Config{
		Key: keyEncryptionKey,
	})
	if n != 64 || err != nil {
		return nil, errors.New("failed to seal object encryption key") // if this happens there's a bug in the code (may panic ?)
	}

	metadata[crypto.SSEIV] = base64.StdEncoding.EncodeToString(iv[:])
	metadata[crypto.SSESealAlgorithm] = SSESealAlgorithmDareV2HmacSha256
	if sseS3 {
		metadata[crypto.S3SealedKey] = base64.StdEncoding.EncodeToString(sealedKey.Bytes())
	} else {
		metadata[crypto.SSECSealedKey] = base64.StdEncoding.EncodeToString(sealedKey.Bytes())

	}

	return objectEncryptionKey, nil
}

func newEncryptReader(content io.Reader, key []byte, bucket, object string, metadata map[string]string, sseS3 bool) (io.Reader, error) {
	objectEncryptionKey, err := newEncryptMetadata(key, bucket, object, metadata, sseS3)
	if err != nil {
		return nil, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}

	return reader, nil
}

// generateKMSKey gets a new random plaintext key and sealed plaintext key from
// Vault. It returns the plaintext key and stores the master Key ID and
// the sealed plaintext key in the metadata map on success.
func generateKMSKey(r *http.Request, bucket, object string, metadata map[string]string) ([]byte, error) {
	if globalKMS == nil {
		return nil, errKMSNotConfigured
	}
	if crypto.S3.IsRequested(r.Header) && crypto.SSEC.IsRequested(r.Header) {
		return nil, crypto.ErrIncompatibleEncryptionMethod
	}
	ctx := crypto.Context{bucket: path.Join(bucket, object)}
	key, encKey, err := globalKMS.GenerateKey(globalKMSKeyID, ctx)
	if err != nil {
		return nil, err
	}
	metadata[crypto.S3KMSKeyID] = globalKMSKeyID
	metadata[crypto.S3KMSSealedKey] = string(encKey)
	return key[:], nil
}

// set new encryption metadata from http request headers for SSE-C and generated key from KMS in the case of
// SSE-S3
func setEncryptionMetadata(r *http.Request, bucket, object string, metadata map[string]string) (err error) {
	var (
		key []byte
	)
	if crypto.SSEC.IsRequested(r.Header) {
		key, err = ParseSSECustomerRequest(r)
	}
	if crypto.S3.IsRequested(r.Header) {
		key, err = generateKMSKey(r, bucket, object, metadata)
	}
	if err != nil {
		return
	}
	_, err = newEncryptMetadata(key, bucket, object, metadata, crypto.S3.IsRequested(r.Header))
	return
}

// EncryptRequest takes the client provided content and encrypts the data
// with the client provided key. It also marks the object as client-side-encrypted
// and sets the correct headers.
func EncryptRequest(content io.Reader, r *http.Request, bucket, object string, metadata map[string]string) (io.Reader, error) {

	var (
		key []byte
		err error
	)
	if crypto.S3.IsRequested(r.Header) && crypto.SSEC.IsRequested(r.Header) {
		return nil, crypto.ErrIncompatibleEncryptionMethod
	}
	if crypto.S3.IsRequested(r.Header) {
		key, err = generateKMSKey(r, bucket, object, metadata)
		if err != nil {
			return nil, err
		}
	}
	if crypto.SSEC.IsRequested(r.Header) {
		key, err = ParseSSECustomerRequest(r)

	}
	if err != nil {
		return nil, err
	}
	return newEncryptReader(content, key, bucket, object, metadata, crypto.S3.IsEncrypted(metadata))
}

// DecryptCopyRequest decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptCopyRequest(client io.Writer, r *http.Request, bucket, object string, metadata map[string]string) (io.WriteCloser, error) {
	var (
		key []byte
		err error
	)
	if crypto.S3.IsEncrypted(metadata) {
		key, err = unsealKMSKey(bucket, object, metadata)
	}
	if crypto.SSECopy.IsRequested(r.Header) {
		key, err = ParseSSECopyCustomerRequest(r, metadata)
	}

	if err != nil {
		return nil, err
	}
	delete(metadata, crypto.SSECopyKey) // make sure we do not save the key by accident
	return newDecryptWriter(client, key, bucket, object, 0, metadata)
}

func decryptObjectInfo(key []byte, bucket, object string, metadata map[string]string) ([]byte, error) {
	iv, err := base64.StdEncoding.DecodeString(metadata[crypto.SSEIV])
	if err != nil || len(iv) != SSEIVSize {
		return nil, errObjectTampered
	}
	k := crypto.SSECSealedKey
	if crypto.S3.IsEncrypted(metadata) {
		k = crypto.S3SealedKey
	}
	sealedKey, err := base64.StdEncoding.DecodeString(metadata[k])
	if err != nil || len(sealedKey) != 64 {
		return nil, errObjectTampered
	}

	var (
		minDAREVersion   byte
		keyEncryptionKey [32]byte
	)
	switch algorithm := metadata[crypto.SSESealAlgorithm]; algorithm {
	default:
		return nil, errObjectTampered
	case SSESealAlgorithmDareSha256: // legacy key-encryption-key derivation
		minDAREVersion = sio.Version10
		sha := sha256.New()
		sha.Write(key)
		sha.Write(iv)
		sha.Sum(keyEncryptionKey[:0])
	case SSESealAlgorithmDareV2HmacSha256: // key-encryption-key derivation - See: crypto/doc.go
		minDAREVersion = sio.Version20
		mac := hmac.New(sha256.New, key)
		mac.Write(iv)
		mac.Write([]byte(SSEDomain))
		mac.Write([]byte(SSESealAlgorithmDareV2HmacSha256))
		mac.Write([]byte(path.Join(bucket, object)))
		mac.Sum(keyEncryptionKey[:0])
	}

	objectEncryptionKey := bytes.NewBuffer(nil) // decrypt object encryption key
	n, err := sio.Decrypt(objectEncryptionKey, bytes.NewReader(sealedKey), sio.Config{
		MinVersion: minDAREVersion,
		Key:        keyEncryptionKey[:],
	})
	if n != 32 || err != nil {
		// Either the provided key does not match or the object was tampered.
		// To provide strict AWS S3 compatibility we return: access denied.
		return nil, crypto.ErrInvalidCustomerKey
	}
	return objectEncryptionKey.Bytes(), nil
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

// unsealKMSKey unseals the sealedKey using the Vault master key
// referenced by the keyID. The plain text key is returned on success.
func unsealKMSKey(bucket, object string, metadata map[string]string) ([]byte, error) {
	if globalKMS == nil {
		return nil, errKMSNotConfigured
	}
	ctx := crypto.Context{bucket: path.Join(bucket, object)}
	keyID, _ := metadata[crypto.S3KMSKeyID]
	sealedKey, _ := metadata[crypto.S3KMSSealedKey]
	k, err := globalKMS.UnsealKey(keyID, []byte(sealedKey), ctx)
	if err != nil {
		return nil, err
	}
	return k[:], nil
}

// DecryptRequestWithSequenceNumber decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptRequestWithSequenceNumber(client io.Writer, r *http.Request, bucket, object string, seqNumber uint32, metadata map[string]string) (io.WriteCloser, error) {
	if crypto.S3.IsEncrypted(metadata) {
		key, err := unsealKMSKey(bucket, object, metadata)
		if err != nil {
			return nil, err
		}
		delete(metadata, crypto.S3KMSKeyID)
		delete(metadata, crypto.S3KMSSealedKey)
		return newDecryptWriter(client, key, bucket, object, seqNumber, metadata)
	}

	key, err := ParseSSECustomerRequest(r)
	if err != nil {
		return nil, err
	}
	delete(metadata, crypto.SSECKey) // make sure we do not save the key by accident

	return newDecryptWriter(client, key, bucket, object, seqNumber, metadata)
}

// DecryptRequest decrypts the object with client provided key for SSE-C and SSE-S3. It also removes
// the encryption metadata from the object and sets the correct headers.
func DecryptRequest(client io.Writer, r *http.Request, bucket, object string, metadata map[string]string) (io.WriteCloser, error) {
	return DecryptRequestWithSequenceNumber(client, r, bucket, object, 0, metadata)
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
		if crypto.S3.IsEncrypted(w.metadata) {
			key, err = unsealKMSKey(w.bucket, w.object, w.metadata)
		} else {
			w.req.Header.Set(crypto.SSECopyKey, w.customerKeyHeader)
			key, err = ParseSSECopyCustomerRequest(w.req, w.metadata)
		}
	} else {
		if crypto.S3.IsEncrypted(w.metadata) {
			key, err = unsealKMSKey(w.bucket, w.object, w.metadata)
		} else {
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

	// make sure we do not save the key by accident
	if w.copySource {
		delete(m, crypto.SSECopyKey)
	} else {
		delete(m, crypto.SSECKey)
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
func DecryptAllBlocksCopyRequest(client io.Writer, r *http.Request, bucket, object string, objInfo ObjectInfo) (io.WriteCloser, int64, error) {
	w, _, size, err := DecryptBlocksRequest(client, r, bucket, object, 0, objInfo.Size, objInfo, true)
	return w, size, err
}

// DecryptBlocksRequest - setup a struct which can decrypt many concatenated encrypted data
// parts information helps to know the boundaries of each encrypted data block.
func DecryptBlocksRequest(client io.Writer, r *http.Request, bucket, object string, startOffset, length int64, objInfo ObjectInfo, copySource bool) (io.WriteCloser, int64, int64, error) {
	var seqNumber uint32
	var encStartOffset, encLength int64

	if len(objInfo.Parts) == 0 || !crypto.IsMultiPart(objInfo.UserDefined) {
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
	if len(o.Parts) == 0 || !crypto.IsMultiPart(o.UserDefined) {
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
	// disallow X-Amz-Server-Side-Encryption header on HEAD and GET
	if crypto.S3.IsRequested(headers) {
		apiErr = ErrInvalidEncryptionParameters
		return
	}
	if apiErr, encrypted = ErrNone, crypto.IsEncrypted(info.UserDefined); !encrypted && crypto.SSEC.IsRequested(headers) {
		apiErr = ErrInvalidEncryptionParameters
	} else if encrypted {
		if (crypto.SSEC.IsEncrypted(info.UserDefined) && !crypto.SSEC.IsRequested(headers)) ||
			(crypto.S3.IsEncrypted(info.UserDefined) && crypto.SSEC.IsRequested(headers)) {
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
