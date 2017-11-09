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
	"crypto/hmac"
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

// SSE-C key derivation:
// H: Hash function, M: MAC function
//
// key     := 32 bytes              # client provided key
// r       := H(random(32 bytes))   # saved as object metadata [ServerSideEncryptionIV]
// key_mac := M(H(key), r)          # saved as object metadata [ServerSideEncryptionKeyMAC]
// enc_key := M(key, key_mac)
//
//
// SSE-C key verification:
// H: Hash function, M: MAC function
//
// key      := 32 bytes             # client provided key
// r        := object metadata [ServerSideEncryptionIV]
// key_mac  := object metadata [ServerSideEncryptionKeyMAC]
// key_mac' := M(H(key), r)
//
// check:   key_mac != key_mac' => fail with invalid key
//
// enc_key  := M(key, key_mac')
const (
	// ServerSideEncryptionIV is a 32 byte randomly generated IV used to derive an
	// unique encryption key from the client provided key. The combination of this value
	// and the client-provided key must be unique to provide the DARE tamper-proof property.
	ServerSideEncryptionIV = ReservedMetadataPrefix + "Server-Side-Encryption-Iv"

	// ServerSideEncryptionKDF is the combination of a hash and MAC function used to derive
	// the SSE-C encryption key from the user-provided key.
	ServerSideEncryptionKDF = ReservedMetadataPrefix + "Server-Side-Encryption-Kdf"

	// ServerSideEncryptionKeyMAC is the MAC of the hash of the client-provided key and the
	// X-Minio-Server-Side-Encryption-Iv. This value must be used to verify that the client
	// provided the correct key to follow S3 spec.
	ServerSideEncryptionKeyMAC = ReservedMetadataPrefix + "Server-Side-Encryption-Key-Mac"
)

// SSEKeyDerivationHmacSha256 specifies SHA-256 as hash function and HMAC-SHA256 as MAC function
// as the functions used to derive the SSE-C encryption keys from the client-provided key.
const SSEKeyDerivationHmacSha256 = "HMAC-SHA256"

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
	// Reusing a tuple (nonce, client provided key) will produce the same encryption key
	// twice and breaks the tamper-proof property. However objects are still confidential.
	// Therefore the nonce must be unique but need not to be undistinguishable from true
	// randomness.
	nonce := make([]byte, 32) // generate random nonce to derive encryption key
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	iv := sha256.Sum256(nonce) // hash output to not reveal any stat. weaknesses of the PRNG

	keyHash := sha256.Sum256(key) // derive MAC of the client-provided key
	mac := hmac.New(sha256.New, keyHash[:])
	mac.Write(iv[:])
	keyMAC := mac.Sum(nil)

	mac = hmac.New(sha256.New, key) // derive encryption key
	mac.Write(keyMAC)
	reader, err := sio.EncryptReader(content, sio.Config{Key: mac.Sum(nil)})
	if err != nil {
		return nil, errInvalidSSEKey
	}

	metadata[ServerSideEncryptionIV] = base64.StdEncoding.EncodeToString(iv[:])
	metadata[ServerSideEncryptionKDF] = SSEKeyDerivationHmacSha256
	metadata[ServerSideEncryptionKeyMAC] = base64.StdEncoding.EncodeToString(keyMAC)
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

	if metadata[ServerSideEncryptionKDF] != SSEKeyDerivationHmacSha256 { // currently HMAC-SHA256 is the only option
		return nil, errObjectTampered
	}
	nonce, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionIV])
	if err != nil || len(nonce) != 32 {
		return nil, errObjectTampered
	}
	keyMAC, err := base64.StdEncoding.DecodeString(metadata[ServerSideEncryptionKeyMAC])
	if err != nil || len(keyMAC) != 32 {
		return nil, errObjectTampered
	}

	keyHash := sha256.Sum256(key) // verify that client provided correct key
	mac := hmac.New(sha256.New, keyHash[:])
	mac.Write(nonce)
	if !hmac.Equal(keyMAC, mac.Sum(nil)) {
		return nil, errSSEKeyMismatch // client-provided key is wrong or object metadata was modified
	}

	mac = hmac.New(sha256.New, key) // derive decryption key
	mac.Write(keyMAC)
	writer, err := sio.DecryptWriter(client, sio.Config{Key: mac.Sum(nil)})
	if err != nil {
		return nil, errInvalidSSEKey
	}

	delete(metadata, ServerSideEncryptionIV)
	delete(metadata, ServerSideEncryptionKDF)
	delete(metadata, ServerSideEncryptionKeyMAC)
	return writer, nil
}

// IsEncrypted returns true if the object is marked as encrypted.
func (o *ObjectInfo) IsEncrypted() bool {
	if _, ok := o.UserDefined[ServerSideEncryptionIV]; ok {
		return true
	}
	if _, ok := o.UserDefined[ServerSideEncryptionKDF]; ok {
		return true
	}
	if _, ok := o.UserDefined[ServerSideEncryptionKeyMAC]; ok {
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
