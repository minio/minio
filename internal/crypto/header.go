// Copyright (c) 2015-2021 MinIO, Inc.
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

package crypto

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"net/http"

	xhttp "github.com/minio/minio/internal/http"
)

// RemoveSensitiveHeaders removes confidential encryption
// information - e.g. the SSE-C key - from the HTTP headers.
// It has the same semantics as RemoveSensitiveEntires.
func RemoveSensitiveHeaders(h http.Header) {
	h.Del(xhttp.AmzServerSideEncryptionCustomerKey)
	h.Del(xhttp.AmzServerSideEncryptionCopyCustomerKey)
	h.Del(xhttp.AmzMetaUnencryptedContentLength)
	h.Del(xhttp.AmzMetaUnencryptedContentMD5)
}

var (
	// SSECopy represents AWS SSE-C for copy requests. It provides
	// functionality to handle SSE-C copy requests.
	SSECopy = ssecCopy{}
)

type ssecCopy struct{}

// IsRequested returns true if the HTTP headers contains
// at least one SSE-C copy header. Regular SSE-C headers
// are ignored.
func (ssecCopy) IsRequested(h http.Header) bool {
	if _, ok := h[xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzServerSideEncryptionCopyCustomerKey]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5]; ok {
		return true
	}
	return false
}

// ParseHTTP parses the SSE-C copy headers and returns the SSE-C client key
// on success. Regular SSE-C headers are ignored.
func (ssecCopy) ParseHTTP(h http.Header) (key [32]byte, err error) {
	if h.Get(xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm) != xhttp.AmzEncryptionAES {
		return key, ErrInvalidCustomerAlgorithm
	}
	if h.Get(xhttp.AmzServerSideEncryptionCopyCustomerKey) == "" {
		return key, ErrMissingCustomerKey
	}
	if h.Get(xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5) == "" {
		return key, ErrMissingCustomerKeyMD5
	}

	clientKey, err := base64.StdEncoding.DecodeString(h.Get(xhttp.AmzServerSideEncryptionCopyCustomerKey))
	if err != nil || len(clientKey) != 32 { // The client key must be 256 bits long
		return key, ErrInvalidCustomerKey
	}
	keyMD5, err := base64.StdEncoding.DecodeString(h.Get(xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5))
	if md5Sum := md5.Sum(clientKey); err != nil || !bytes.Equal(md5Sum[:], keyMD5) {
		return key, ErrCustomerKeyMD5Mismatch
	}
	copy(key[:], clientKey)
	return key, nil
}
