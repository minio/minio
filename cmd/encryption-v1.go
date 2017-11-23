/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"

	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

var (
	// AWS errors for invalid SSE-C requests.
	errInsecureSSERequest  = errors.New("Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection")
	errEncryptedObject     = errors.New("The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object")
	errInvalidSSEAlgorithm = errors.New("Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm")
	errMissingSSEKey       = errors.New("Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key")
	errInvalidSSEKey       = errors.New("The secret key was invalid for the specified algorithm")
	errMissingSSEKeyMD5    = errors.New("Requests specifying Server Side Encryption with Customer provided keys must provide the client calculated MD5 of the secret key")
	errSSEKeyMD5Mismatch   = errors.New("The calculated MD5 hash of the key did not match the hash that was provided")
	errSSEKeyMismatch      = errors.New("The client provided key does not match the key provided when the object was encrypted") // this msg is not shown to the client

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
)

const (
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

// IsSSECustomerRequest returns true if the given HTTP header
// contains server-side-encryption with customer provided key fields.
func IsSSECustomerRequest(header http.Header) bool {
	return header.Get(SSECustomerAlgorithm) != "" || header.Get(SSECustomerKey) != "" || header.Get(SSECustomerKeyMD5) != ""
}

// ParseSSECustomerRequest parses the SSE-C header fields of the provided request.
// It returns the client provided key on success.
func ParseSSECustomerRequest(r *http.Request) (key []byte, err error) {
	if !globalIsSSL { // minio only supports HTTP or HTTPS requests not both at the same time
		// we cannot use r.TLS == nil here because Go's http implementation reflects on
		// the net.Conn and sets the TLS field of http.Request only if it's an tls.Conn.
		// Minio uses a BufConn (wrapping a tls.Conn) so the type check within the http package
		// will always fail -> r.TLS is always nil even for TLS requests.
		return nil, errInsecureSSERequest
	}
	header := r.Header
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
	header.Del(SSECustomerKey) // make sure we do not save the key by accident

	if len(key) != SSECustomerKeySize {
		return nil, errInvalidSSEKey
	}

	keyMD5, err := base64.StdEncoding.DecodeString(header.Get(SSECustomerKeyMD5))
	if err != nil {
		return nil, errSSEKeyMD5Mismatch
	}
	if md5Sum := md5.Sum(key); !bytes.Equal(md5Sum[:], keyMD5) {
		return nil, errSSEKeyMD5Mismatch
	}
	return key, nil
}

// EncryptRequest takes the client provided content and encrypts the data
// with the client provided key. It also marks the object as client-side-encrypted
// and sets the correct headers.
func EncryptRequest(content io.Reader, r *http.Request, metadata map[string]string) (io.Reader, error) {
	key, err := ParseSSECustomerRequest(r)
	if err != nil {
		return nil, err
	}
	delete(metadata, SSECustomerKey) // make sure we do not save the key by accident

	// security notice:
	//  - If the first 32 bytes of the random value are ever repeated under the same client-provided
	//    key the encrypted object will not be tamper-proof. [ P(coll) ~= 1 / 2^(256 / 2)]
	//  - If the last 32 bytes of the random value are ever repeated under the same client-provided
	//    key an adversary may be able to extract the object encryption key. This depends on the
	//    authenticated en/decryption scheme. The DARE format will generate an 8 byte nonce which must
	//    be repeated in addition to reveal the object encryption key.
	//    [ P(coll) ~= 1 / 2^((256 + 64) / 2) ]
	nonce := make([]byte, 64) // generate random values for key derivation
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
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

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey})
	if err != nil {
		return nil, errInvalidSSEKey
	}

	metadata[ServerSideEncryptionIV] = base64.StdEncoding.EncodeToString(iv[:])
	metadata[ServerSideEncryptionSealAlgorithm] = SSESealAlgorithmDareSha256
	metadata[ServerSideEncryptionSealedKey] = base64.StdEncoding.EncodeToString(sealedKey.Bytes())
	return reader, nil
}

// DecryptRequest decrypts the object with the client provided key. It also removes
// the client-side-encryption metadata from the object and sets the correct headers.
func DecryptRequest(client io.Writer, r *http.Request, metadata map[string]string) (io.WriteCloser, error) {
	key, err := ParseSSECustomerRequest(r)
	if err != nil {
		return nil, err
	}
	delete(metadata, SSECustomerKey) // make sure we do not save the key by accident

	if metadata[ServerSideEncryptionSealAlgorithm] != SSESealAlgorithmDareSha256 { // currently DARE-SHA256 is the only option
		return nil, errObjectTampered
	}
	iv, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionIV])
	if err != nil || len(iv) != 32 {
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

	writer, err := sio.DecryptWriter(client, sio.Config{Key: objectEncryptionKey.Bytes()})
	if err != nil {
		return nil, errInvalidSSEKey
	}

	delete(metadata, ServerSideEncryptionIV)
	delete(metadata, ServerSideEncryptionSealAlgorithm)
	delete(metadata, ServerSideEncryptionSealedKey)
	return writer, nil
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

// DecryptedSize returns the size of the object after decryption in bytes.
// It returns an error if the object is not encrypted or marked as encrypted
// but has an invalid size.
// DecryptedSize panics if the referred object is not encrypted.
func (o *ObjectInfo) DecryptedSize() (int64, error) {
	if !o.IsEncrypted() {
		panic("cannot compute decrypted size of an object which is not encrypted")
	}
	if o.Size == 0 {
		return o.Size, nil
	}
	size := (o.Size / (32 + 64*1024)) * (64 * 1024)
	if mod := o.Size % (32 + 64*1024); mod > 0 {
		if mod < 33 {
			return -1, errObjectTampered // object is not 0 size but smaller than the smallest valid encrypted object
		}
		size += mod - 32
	}
	return size, nil
}

// EncryptedSize returns the size of the object after encryption.
// An encrypted object is always larger than a plain object
// except for zero size objects.
func (o *ObjectInfo) EncryptedSize() int64 {
	size := (o.Size / (64 * 1024)) * (32 + 64*1024)
	if mod := o.Size % (64 * 1024); mod > 0 {
		size += mod + 32
	}
	return size
}

// DecryptObjectInfo tries to decrypt the provided object if it is encrypted.
// It fails if the object is encrypted and the HTTP headers don't contain
// SSE-C headers or the object is not encrypted but SSE-C headers are provided. (AWS behavior)
// DecryptObjectInfo returns 'ErrNone' if the object is not encrypted or the
// decryption succeeded.
//
// DecryptObjectInfo also returns whether the object is encrypted or not.
func DecryptObjectInfo(info *ObjectInfo, headers http.Header) (apiErr APIErrorCode, encrypted bool) {
	if apiErr, encrypted = ErrNone, info.IsEncrypted(); !encrypted && IsSSECustomerRequest(headers) {
		apiErr = ErrInvalidEncryptionParameters
	} else if encrypted {
		if !IsSSECustomerRequest(headers) {
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
