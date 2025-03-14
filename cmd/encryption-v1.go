// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/minio/kms-go/kes"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/etag"
	"github.com/minio/minio/internal/fips"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/sio"
)

var (
	// AWS errors for invalid SSE-C requests.
	errEncryptedObject                = errors.New("The object was stored using a form of SSE")
	errInvalidSSEParameters           = errors.New("The SSE-C key for key-rotation is not correct") // special access denied
	errKMSNotConfigured               = errors.New("KMS not configured for a server side encrypted objects")
	errKMSKeyNotFound                 = errors.New("Unknown KMS key ID")
	errKMSDefaultKeyAlreadyConfigured = errors.New("A default encryption already exists on KMS")
	// Additional MinIO errors for SSE-C requests.
	errObjectTampered = errors.New("The requested object was modified and may be compromised")
	// error returned when invalid encryption parameters are specified
	errInvalidEncryptionParameters     = errors.New("The encryption parameters are not applicable to this object")
	errInvalidEncryptionParametersSSEC = errors.New("SSE-C encryption parameters are not supported on this bucket")
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

// KMSKeyID returns in AWS compatible KMS KeyID() format.
func (o *ObjectInfo) KMSKeyID() string { return kmsKeyIDFromMetadata(o.UserDefined) }

// KMSKeyID returns in AWS compatible KMS KeyID() format.
func (o *MultipartInfo) KMSKeyID() string { return kmsKeyIDFromMetadata(o.UserDefined) }

// kmsKeyIDFromMetadata returns any AWS S3 KMS key ID in the
// metadata, if any. It returns an empty ID if no key ID is
// present.
func kmsKeyIDFromMetadata(metadata map[string]string) string {
	const ARNPrefix = crypto.ARNPrefix
	if len(metadata) == 0 {
		return ""
	}
	kmsID, ok := metadata[crypto.MetaKeyID]
	if !ok {
		return ""
	}
	if strings.HasPrefix(kmsID, ARNPrefix) {
		return kmsID
	}
	return ARNPrefix + kmsID
}

// DecryptETags decryptes the ETag of all ObjectInfos using the KMS.
//
// It adjusts the size of all encrypted objects since encrypted
// objects are slightly larger due to encryption overhead.
// Further, it decrypts all single-part SSE-S3 encrypted objects
// and formats ETags of SSE-C / SSE-KMS encrypted objects to
// be AWS S3 compliant.
//
// DecryptETags uses a KMS bulk decryption API, if available, which
// is more efficient than decrypting ETags sequentially.
func DecryptETags(ctx context.Context, k *kms.KMS, objects []ObjectInfo) error {
	const BatchSize = 250 // We process the objects in batches - 250 is a reasonable default.
	var (
		metadata = make([]map[string]string, 0, BatchSize)
		buckets  = make([]string, 0, BatchSize)
		names    = make([]string, 0, BatchSize)
	)
	for len(objects) > 0 {
		N := BatchSize
		if len(objects) < BatchSize {
			N = len(objects)
		}
		batch := objects[:N]

		// We have to decrypt only ETags of SSE-S3 single-part
		// objects.
		// Therefore, we remember which objects (there index)
		// in the current batch are single-part SSE-S3 objects.
		metadata = metadata[:0:N]
		buckets = buckets[:0:N]
		names = names[:0:N]
		SSES3SinglePartObjects := make(map[int]bool)
		for i, object := range batch {
			if kind, ok := crypto.IsEncrypted(object.UserDefined); ok && kind == crypto.S3 && !crypto.IsMultiPart(object.UserDefined) {
				ETag, err := etag.Parse(object.ETag)
				if err != nil {
					continue
				}
				if ETag.IsEncrypted() {
					SSES3SinglePartObjects[i] = true
					metadata = append(metadata, object.UserDefined)
					buckets = append(buckets, object.Bucket)
					names = append(names, object.Name)
				}
			}
		}

		// If there are no SSE-S3 single-part objects
		// we can skip the decryption process. However,
		// we still have to adjust the size and ETag
		// of SSE-C and SSE-KMS objects.
		if len(SSES3SinglePartObjects) == 0 {
			for i := range batch {
				size, err := batch[i].GetActualSize()
				if err != nil {
					return err
				}
				batch[i].Size = size

				if _, ok := crypto.IsEncrypted(batch[i].UserDefined); ok {
					ETag, err := etag.Parse(batch[i].ETag)
					if err != nil {
						return err
					}
					batch[i].ETag = ETag.Format().String()
				}
			}
			objects = objects[N:]
			continue
		}

		// There is at least one SSE-S3 single-part object.
		// For all SSE-S3 single-part objects we have to
		// fetch their decryption keys. We do this using
		// a Bulk-Decryption API call, if available.
		keys, err := crypto.S3.UnsealObjectKeys(ctx, k, metadata, buckets, names)
		if err != nil {
			return err
		}

		// Now, we have to decrypt the ETags of SSE-S3 single-part
		// objects and adjust the size and ETags of all encrypted
		// objects.
		for i := range batch {
			size, err := batch[i].GetActualSize()
			if err != nil {
				return err
			}
			batch[i].Size = size

			if _, ok := crypto.IsEncrypted(batch[i].UserDefined); ok {
				ETag, err := etag.Parse(batch[i].ETag)
				if err != nil {
					return err
				}
				if SSES3SinglePartObjects[i] {
					ETag, err = etag.Decrypt(keys[0][:], ETag)
					if err != nil {
						return err
					}
					keys = keys[1:]
				}
				batch[i].ETag = ETag.Format().String()
			}
		}
		objects = objects[N:]
	}
	return nil
}

// isMultipart returns true if the current object is
// uploaded by the user using multipart mechanism:
// initiate new multipart, upload part, complete upload
func (o *ObjectInfo) isMultipart() bool {
	_, encrypted := crypto.IsEncrypted(o.UserDefined)
	if encrypted {
		if !crypto.IsMultiPart(o.UserDefined) {
			return false
		}
		for _, part := range o.Parts {
			_, err := sio.DecryptedSize(uint64(part.Size))
			if err != nil {
				return false
			}
		}
	}

	// Further check if this object is uploaded using multipart mechanism
	// by the user and it is not about Erasure internally splitting the
	// object into parts in PutObject()
	return len(o.ETag) != 32
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
func rotateKey(ctx context.Context, oldKey []byte, newKeyID string, newKey []byte, bucket, object string, metadata map[string]string, cryptoCtx kms.Context) error {
	kind, _ := crypto.IsEncrypted(metadata)
	switch kind {
	case crypto.S3:
		if GlobalKMS == nil {
			return errKMSNotConfigured
		}
		keyID, kmsKey, sealedKey, err := crypto.S3.ParseMetadata(metadata)
		if err != nil {
			return err
		}
		oldKey, err := GlobalKMS.Decrypt(ctx, &kms.DecryptRequest{
			Name:           keyID,
			Ciphertext:     kmsKey,
			AssociatedData: kms.Context{bucket: path.Join(bucket, object)},
		})
		if err != nil {
			return err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(oldKey, sealedKey, crypto.S3.String(), bucket, object); err != nil {
			return err
		}

		newKey, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{
			Name:           GlobalKMS.DefaultKey,
			AssociatedData: kms.Context{bucket: path.Join(bucket, object)},
		})
		if err != nil {
			return err
		}
		sealedKey = objectKey.Seal(newKey.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
		crypto.S3.CreateMetadata(metadata, newKey.KeyID, newKey.Ciphertext, sealedKey)
		return nil
	case crypto.S3KMS:
		if GlobalKMS == nil {
			return errKMSNotConfigured
		}
		objectKey, err := crypto.S3KMS.UnsealObjectKey(GlobalKMS, metadata, bucket, object)
		if err != nil {
			return err
		}

		if len(cryptoCtx) == 0 {
			_, _, _, cryptoCtx, err = crypto.S3KMS.ParseMetadata(metadata)
			if err != nil {
				return err
			}
		}

		// If the context does not contain the bucket key
		// we must add it for key generation. However,
		// the context must be stored exactly like the
		// client provided it. Therefore, we create a copy
		// of the client provided context and add the bucket
		// key, if not present.
		kmsCtx := kms.Context{}
		for k, v := range cryptoCtx {
			kmsCtx[k] = v
		}
		if _, ok := kmsCtx[bucket]; !ok {
			kmsCtx[bucket] = path.Join(bucket, object)
		}
		newKey, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{
			Name:           newKeyID,
			AssociatedData: kmsCtx,
		})
		if err != nil {
			return err
		}

		sealedKey := objectKey.Seal(newKey.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3KMS.String(), bucket, object)
		crypto.S3KMS.CreateMetadata(metadata, newKey.KeyID, newKey.Ciphertext, sealedKey, cryptoCtx)
		return nil
	case crypto.SSEC:
		sealedKey, err := crypto.SSEC.ParseMetadata(metadata)
		if err != nil {
			return err
		}

		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(oldKey, sealedKey, crypto.SSEC.String(), bucket, object); err != nil {
			if subtle.ConstantTimeCompare(oldKey, newKey) == 1 {
				return errInvalidSSEParameters // AWS returns special error for equal but invalid keys.
			}
			return crypto.ErrInvalidCustomerKey // To provide strict AWS S3 compatibility we return: access denied.
		}

		if subtle.ConstantTimeCompare(oldKey, newKey) == 1 && sealedKey.Algorithm == crypto.SealAlgorithm {
			return nil // don't rotate on equal keys if seal algorithm is latest
		}
		sealedKey = objectKey.Seal(newKey, sealedKey.IV, crypto.SSEC.String(), bucket, object)
		crypto.SSEC.CreateMetadata(metadata, sealedKey)
		return nil
	default:
		return errObjectTampered
	}
}

func newEncryptMetadata(ctx context.Context, kind crypto.Type, keyID string, key []byte, bucket, object string, metadata map[string]string, cryptoCtx kms.Context) (crypto.ObjectKey, error) {
	var sealedKey crypto.SealedKey
	switch kind {
	case crypto.S3:
		if GlobalKMS == nil {
			return crypto.ObjectKey{}, errKMSNotConfigured
		}
		key, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{
			AssociatedData: kms.Context{bucket: path.Join(bucket, object)},
		})
		if err != nil {
			return crypto.ObjectKey{}, err
		}

		objectKey := crypto.GenerateKey(key.Plaintext, rand.Reader)
		sealedKey = objectKey.Seal(key.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3.String(), bucket, object)
		crypto.S3.CreateMetadata(metadata, key.KeyID, key.Ciphertext, sealedKey)
		return objectKey, nil
	case crypto.S3KMS:
		if GlobalKMS == nil {
			return crypto.ObjectKey{}, errKMSNotConfigured
		}

		// If the context does not contain the bucket key
		// we must add it for key generation. However,
		// the context must be stored exactly like the
		// client provided it. Therefore, we create a copy
		// of the client provided context and add the bucket
		// key, if not present.
		kmsCtx := kms.Context{}
		for k, v := range cryptoCtx {
			kmsCtx[k] = v
		}
		if _, ok := kmsCtx[bucket]; !ok {
			kmsCtx[bucket] = path.Join(bucket, object)
		}
		key, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{
			Name:           keyID,
			AssociatedData: kmsCtx,
		})
		if err != nil {
			if errors.Is(err, kes.ErrKeyNotFound) {
				return crypto.ObjectKey{}, errKMSKeyNotFound
			}
			return crypto.ObjectKey{}, err
		}

		objectKey := crypto.GenerateKey(key.Plaintext, rand.Reader)
		sealedKey = objectKey.Seal(key.Plaintext, crypto.GenerateIV(rand.Reader), crypto.S3KMS.String(), bucket, object)
		crypto.S3KMS.CreateMetadata(metadata, key.KeyID, key.Ciphertext, sealedKey, cryptoCtx)
		return objectKey, nil
	case crypto.SSEC:
		objectKey := crypto.GenerateKey(key, rand.Reader)
		sealedKey = objectKey.Seal(key, crypto.GenerateIV(rand.Reader), crypto.SSEC.String(), bucket, object)
		crypto.SSEC.CreateMetadata(metadata, sealedKey)
		return objectKey, nil
	default:
		return crypto.ObjectKey{}, fmt.Errorf("encryption type '%v' not supported", kind)
	}
}

func newEncryptReader(ctx context.Context, content io.Reader, kind crypto.Type, keyID string, key []byte, bucket, object string, metadata map[string]string, cryptoCtx kms.Context) (io.Reader, crypto.ObjectKey, error) {
	objectEncryptionKey, err := newEncryptMetadata(ctx, kind, keyID, key, bucket, object, metadata, cryptoCtx)
	if err != nil {
		return nil, crypto.ObjectKey{}, err
	}

	reader, err := sio.EncryptReader(content, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20, CipherSuites: fips.DARECiphers()})
	if err != nil {
		return nil, crypto.ObjectKey{}, crypto.ErrInvalidCustomerKey
	}

	return reader, objectEncryptionKey, nil
}

// set new encryption metadata from http request headers for SSE-C and generated key from KMS in the case of
// SSE-S3
func setEncryptionMetadata(r *http.Request, bucket, object string, metadata map[string]string) (err error) {
	var (
		key    []byte
		keyID  string
		kmsCtx kms.Context
	)
	kind, _ := crypto.IsRequested(r.Header)
	switch kind {
	case crypto.SSEC:
		key, err = ParseSSECustomerRequest(r)
		if err != nil {
			return err
		}
	case crypto.S3KMS:
		keyID, kmsCtx, err = crypto.S3KMS.ParseHTTP(r.Header)
		if err != nil {
			return err
		}
	}
	_, err = newEncryptMetadata(r.Context(), kind, keyID, key, bucket, object, metadata, kmsCtx)
	return
}

// EncryptRequest takes the client provided content and encrypts the data
// with the client provided key. It also marks the object as client-side-encrypted
// and sets the correct headers.
func EncryptRequest(content io.Reader, r *http.Request, bucket, object string, metadata map[string]string) (io.Reader, crypto.ObjectKey, error) {
	if r.ContentLength > encryptBufferThreshold {
		// The encryption reads in blocks of 64KB.
		// We add a buffer on bigger files to reduce the number of syscalls upstream.
		content = bufio.NewReaderSize(content, encryptBufferSize)
	}

	var (
		key   []byte
		keyID string
		ctx   kms.Context
		err   error
	)
	kind, _ := crypto.IsRequested(r.Header)
	if kind == crypto.SSEC {
		key, err = ParseSSECustomerRequest(r)
		if err != nil {
			return nil, crypto.ObjectKey{}, err
		}
	}
	if kind == crypto.S3KMS {
		keyID, ctx, err = crypto.S3KMS.ParseHTTP(r.Header)
		if err != nil {
			return nil, crypto.ObjectKey{}, err
		}
	}
	return newEncryptReader(r.Context(), content, kind, keyID, key, bucket, object, metadata, ctx)
}

func decryptObjectMeta(key []byte, bucket, object string, metadata map[string]string) ([]byte, error) {
	switch kind, _ := crypto.IsEncrypted(metadata); kind {
	case crypto.S3:
		if GlobalKMS == nil {
			return nil, errKMSNotConfigured
		}
		objectKey, err := crypto.S3.UnsealObjectKey(GlobalKMS, metadata, bucket, object)
		if err != nil {
			return nil, err
		}
		return objectKey[:], nil
	case crypto.S3KMS:
		if GlobalKMS == nil {
			return nil, errKMSNotConfigured
		}
		objectKey, err := crypto.S3KMS.UnsealObjectKey(GlobalKMS, metadata, bucket, object)
		if err != nil {
			return nil, err
		}
		return objectKey[:], nil
	case crypto.SSEC:
		sealedKey, err := crypto.SSEC.ParseMetadata(metadata)
		if err != nil {
			return nil, err
		}
		var objectKey crypto.ObjectKey
		if err = objectKey.Unseal(key, sealedKey, crypto.SSEC.String(), bucket, object); err != nil {
			return nil, err
		}
		return objectKey[:], nil
	default:
		return nil, errObjectTampered
	}
}

// Adding support for reader based interface

// DecryptRequestWithSequenceNumberR - same as
// DecryptRequestWithSequenceNumber but with a reader
func DecryptRequestWithSequenceNumberR(client io.Reader, h http.Header, bucket, object string, seqNumber uint32, metadata map[string]string) (io.Reader, error) {
	if crypto.SSEC.IsEncrypted(metadata) {
		key, err := ParseSSECustomerHeader(h)
		if err != nil {
			return nil, err
		}
		return newDecryptReader(client, key, bucket, object, seqNumber, metadata)
	}
	return newDecryptReader(client, nil, bucket, object, seqNumber, metadata)
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
	objectEncryptionKey, err := decryptObjectMeta(key, bucket, object, metadata)
	if err != nil {
		return nil, err
	}
	return newDecryptReaderWithObjectKey(client, objectEncryptionKey, seqNumber)
}

func newDecryptReaderWithObjectKey(client io.Reader, objectEncryptionKey []byte, seqNumber uint32) (io.Reader, error) {
	reader, err := sio.DecryptReader(client, sio.Config{
		Key:            objectEncryptionKey,
		SequenceNumber: seqNumber,
		CipherSuites:   fips.DARECiphers(),
	})
	if err != nil {
		return nil, crypto.ErrInvalidCustomerKey
	}
	return reader, nil
}

// DecryptBlocksRequestR - same as DecryptBlocksRequest but with a
// reader
func DecryptBlocksRequestR(inputReader io.Reader, h http.Header, seqNumber uint32, partStart int, oi ObjectInfo, copySource bool) (io.Reader, error) {
	bucket, object := oi.Bucket, oi.Name
	// Single part case
	if !oi.isMultipart() {
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
		reader:           inputReader,
		startSeqNum:      seqNumber,
		partDecRelOffset: partDecRelOffset,
		partEncRelOffset: partEncRelOffset,
		parts:            oi.Parts,
		partIndex:        partStart,
	}

	// In case of SSE-C, we have to decrypt the OEK using the client-provided key.
	// In case of a SSE-C server-side copy, the client might provide two keys,
	// one for the source and one for the target. This reader is the source.
	var ssecClientKey []byte
	if crypto.SSEC.IsEncrypted(oi.UserDefined) {
		if copySource && crypto.SSECopy.IsRequested(h) {
			key, err := crypto.SSECopy.ParseHTTP(h)
			if err != nil {
				return nil, err
			}
			ssecClientKey = key[:]
		} else {
			key, err := crypto.SSEC.ParseHTTP(h)
			if err != nil {
				return nil, err
			}
			ssecClientKey = key[:]
		}
	}

	// Decrypt the OEK once and reuse it for all subsequent parts.
	objectEncryptionKey, err := decryptObjectMeta(ssecClientKey, bucket, object, oi.UserDefined)
	if err != nil {
		return nil, err
	}
	w.objectEncryptionKey = objectEncryptionKey

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
	parts []ObjectPartInfo

	objectEncryptionKey                []byte
	partDecRelOffset, partEncRelOffset int64
}

func (d *DecryptBlocksReader) buildDecrypter(partID int) error {
	var partIDbin [4]byte
	binary.LittleEndian.PutUint32(partIDbin[:], uint32(partID)) // marshal part ID

	mac := hmac.New(sha256.New, d.objectEncryptionKey) // derive part encryption key from part ID and object key
	mac.Write(partIDbin[:])
	partEncryptionKey := mac.Sum(nil)

	// Limit the reader, so the decryptor doesn't receive bytes
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
func (o ObjectInfo) DecryptedSize() (int64, error) {
	if _, ok := crypto.IsEncrypted(o.UserDefined); !ok {
		return -1, errors.New("Cannot compute decrypted size of an unencrypted object")
	}

	if !o.isMultipart() {
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
			return -1, errObjectTampered
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
// This special case addresses randomly-generated ETags generated
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
	// encrypted content MD5Sum as ETag for both SSE-C and SSE-KMS, we standardize the ETag
	// encryption across SSE-C and SSE-KMS, and only return last 32 bytes for SSE-C
	if (crypto.SSEC.IsEncrypted(objInfo.UserDefined) || crypto.S3KMS.IsEncrypted(objInfo.UserDefined)) && !copySource {
		return objInfo.ETag[len(objInfo.ETag)-32:]
	}

	objectEncryptionKey, err := decryptObjectMeta(key[:], objInfo.Bucket, objInfo.Name, objInfo.UserDefined)
	if err != nil {
		return objInfo.ETag
	}
	return tryDecryptETag(objectEncryptionKey, objInfo.ETag, true)
}

// helper to decrypt Etag given object encryption key and encrypted ETag
func tryDecryptETag(key []byte, encryptedETag string, sses3 bool) string {
	// ETag for SSE-C or SSE-KMS encrypted objects need not be content MD5Sum.While encrypted
	// md5sum is stored internally, return just the last 32 bytes of hex-encoded and
	// encrypted md5sum string for SSE-C
	if !sses3 {
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
	if _, ok := crypto.IsEncrypted(o.UserDefined); !ok {
		err = errors.New("Object is not encrypted")
		return
	}

	if rs == nil {
		// No range, so offsets refer to the whole object.
		return 0, o.Size, 0, 0, 0, nil
	}

	// Assemble slice of (decrypted) part sizes in `sizes`
	var sizes []int64
	var decObjSize int64 // decrypted total object size
	if o.isMultipart() {
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

// DecryptObjectInfo tries to decrypt the provided object if it is encrypted.
// It fails if the object is encrypted and the HTTP headers don't contain
// SSE-C headers or the object is not encrypted but SSE-C headers are provided. (AWS behavior)
// DecryptObjectInfo returns 'ErrNone' if the object is not encrypted or the
// decryption succeeded.
//
// DecryptObjectInfo also returns whether the object is encrypted or not.
func DecryptObjectInfo(info *ObjectInfo, r *http.Request) (encrypted bool, err error) {
	// Directories are never encrypted.
	if info.IsDir {
		return false, nil
	}
	if r == nil {
		return false, errInvalidArgument
	}

	headers := r.Header

	// disallow X-Amz-Server-Side-Encryption header on HEAD and GET
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if crypto.S3.IsRequested(headers) || crypto.S3KMS.IsRequested(headers) {
			return false, errInvalidEncryptionParameters
		}
	}

	_, encrypted = crypto.IsEncrypted(info.UserDefined)
	if !encrypted && crypto.SSEC.IsRequested(headers) && r.Header.Get(xhttp.AmzCopySource) == "" {
		return false, errInvalidEncryptionParameters
	}

	if encrypted {
		if crypto.SSEC.IsEncrypted(info.UserDefined) {
			if !(crypto.SSEC.IsRequested(headers) || crypto.SSECopy.IsRequested(headers)) {
				if r.Header.Get(xhttp.MinIOSourceReplicationRequest) != "true" {
					return encrypted, errEncryptedObject
				}
			}
		}

		if crypto.S3.IsEncrypted(info.UserDefined) && r.Header.Get(xhttp.AmzCopySource) == "" {
			if crypto.SSEC.IsRequested(headers) || crypto.SSECopy.IsRequested(headers) {
				return encrypted, errEncryptedObject
			}
		}

		if crypto.S3KMS.IsEncrypted(info.UserDefined) && r.Header.Get(xhttp.AmzCopySource) == "" {
			if crypto.SSEC.IsRequested(headers) || crypto.SSECopy.IsRequested(headers) {
				return encrypted, errEncryptedObject
			}
		}

		if _, err = info.DecryptedSize(); err != nil {
			return encrypted, err
		}

		if _, ok := crypto.IsEncrypted(info.UserDefined); ok && !crypto.IsMultiPart(info.UserDefined) {
			info.ETag = getDecryptedETag(headers, *info, false)
		}
	}

	return encrypted, nil
}

type (
	objectMetaEncryptFn func(baseKey string, data []byte) []byte
	objectMetaDecryptFn func(baseKey string, data []byte) ([]byte, error)
)

// metadataEncrypter returns a function that will read data from input,
// encrypt it using the provided key and return the result.
// 0 sized inputs are passed through.
func metadataEncrypter(key crypto.ObjectKey) objectMetaEncryptFn {
	return func(baseKey string, data []byte) []byte {
		if len(data) == 0 {
			return data
		}
		var buffer bytes.Buffer
		mac := hmac.New(sha256.New, key[:])
		mac.Write([]byte(baseKey))
		if _, err := sio.Encrypt(&buffer, bytes.NewReader(data), sio.Config{Key: mac.Sum(nil), CipherSuites: fips.DARECiphers()}); err != nil {
			logger.CriticalIf(context.Background(), errors.New("unable to encrypt using object key"))
		}
		return buffer.Bytes()
	}
}

// metadataDecrypter reverses metadataEncrypter.
func (o *ObjectInfo) metadataDecrypter(h http.Header) objectMetaDecryptFn {
	return func(baseKey string, input []byte) ([]byte, error) {
		if len(input) == 0 {
			return input, nil
		}
		var key []byte
		if k, err := crypto.SSEC.ParseHTTP(h); err == nil {
			key = k[:]
		}
		key, err := decryptObjectMeta(key, o.Bucket, o.Name, o.UserDefined)
		if err != nil {
			return nil, err
		}
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(baseKey))
		return sio.DecryptBuffer(nil, input, sio.Config{Key: mac.Sum(nil), CipherSuites: fips.DARECiphers()})
	}
}

// decryptPartsChecksums will attempt to decode checksums and return it/them if set.
// if part > 0, and we have the checksum for the part that will be returned.
func (o *ObjectInfo) decryptPartsChecksums(h http.Header) {
	data := o.Checksum
	if len(data) == 0 {
		return
	}
	if _, encrypted := crypto.IsEncrypted(o.UserDefined); encrypted {
		decrypted, err := o.metadataDecrypter(h)("object-checksum", data)
		if err != nil {
			if !errors.Is(err, crypto.ErrSecretKeyMismatch) {
				encLogIf(GlobalContext, err)
			}
			return
		}
		data = decrypted
	}
	cs := hash.ReadPartCheckSums(data)
	if len(cs) == len(o.Parts) {
		for i := range o.Parts {
			o.Parts[i].Checksums = cs[i]
		}
	}
	return
}

// metadataEncryptFn provides an encryption function for metadata.
// Will return nil, nil if unencrypted.
func (o *ObjectInfo) metadataEncryptFn(headers http.Header) (objectMetaEncryptFn, error) {
	kind, _ := crypto.IsEncrypted(o.UserDefined)
	switch kind {
	case crypto.SSEC:
		if crypto.SSECopy.IsRequested(headers) {
			key, err := crypto.SSECopy.ParseHTTP(headers)
			if err != nil {
				return nil, err
			}
			objectEncryptionKey, err := decryptObjectMeta(key[:], o.Bucket, o.Name, o.UserDefined)
			if err != nil {
				return nil, err
			}
			if len(objectEncryptionKey) == 32 {
				var key crypto.ObjectKey
				copy(key[:], objectEncryptionKey)
				return metadataEncrypter(key), nil
			}
			return nil, errors.New("metadataEncryptFn: unexpected key size")
		}
	case crypto.S3, crypto.S3KMS:
		objectEncryptionKey, err := decryptObjectMeta(nil, o.Bucket, o.Name, o.UserDefined)
		if err != nil {
			return nil, err
		}
		if len(objectEncryptionKey) == 32 {
			var key crypto.ObjectKey
			copy(key[:], objectEncryptionKey)
			return metadataEncrypter(key), nil
		}
		return nil, errors.New("metadataEncryptFn: unexpected key size")
	}

	return nil, nil
}

// decryptChecksums will attempt to decode checksums and return it/them if set.
// if part > 0, and we have the checksum for the part that will be returned.
// Returns whether the checksum (main part 0) is a multipart checksum.
func (o *ObjectInfo) decryptChecksums(part int, h http.Header) (cs map[string]string, isMP bool) {
	data := o.Checksum
	if len(data) == 0 {
		return nil, false
	}
	if part > 0 && !crypto.SSEC.IsEncrypted(o.UserDefined) {
		// already decrypted in ToObjectInfo for multipart objects
		for _, pi := range o.Parts {
			if pi.Number == part {
				return pi.Checksums, true
			}
		}
	}
	if _, encrypted := crypto.IsEncrypted(o.UserDefined); encrypted {
		decrypted, err := o.metadataDecrypter(h)("object-checksum", data)
		if err != nil {
			if err != crypto.ErrSecretKeyMismatch {
				encLogIf(GlobalContext, err)
			}
			return nil, part > 0
		}
		data = decrypted
	}
	return hash.ReadCheckSums(data, part)
}
