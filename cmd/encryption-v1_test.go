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

package cmd

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/sio"
)

var encryptRequestTests = []struct {
	header   map[string]string
	metadata map[string]string
}{
	{
		header: map[string]string{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: "AES256",
			xhttp.AmzServerSideEncryptionCustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{},
	},
	{
		header: map[string]string{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: "AES256",
			xhttp.AmzServerSideEncryptionCustomerKey:       "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    "bY4wkxQejw9mUJfo72k53A==",
		},
		metadata: map[string]string{
			xhttp.AmzServerSideEncryptionCustomerKey: "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=",
		},
	},
}

func TestEncryptRequest(t *testing.T) {
	defer func(flag bool) { globalIsTLS = flag }(globalIsTLS)
	globalIsTLS = true
	for i, test := range encryptRequestTests {
		content := bytes.NewReader(make([]byte, 64))
		req := &http.Request{Header: http.Header{}}
		for k, v := range test.header {
			req.Header.Set(k, v)
		}
		_, _, err := EncryptRequest(content, req, "bucket", "object", test.metadata)
		if err != nil {
			t.Fatalf("Test %d: Failed to encrypt request: %v", i, err)
		}
		if kdf, ok := test.metadata[crypto.MetaAlgorithm]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionKDF must be part of metadata: %v", i, kdf)
		}
		if iv, ok := test.metadata[crypto.MetaIV]; !ok {
			t.Errorf("Test %d: crypto.SSEIV must be part of metadata: %v", i, iv)
		}
		if mac, ok := test.metadata[crypto.MetaSealedKeySSEC]; !ok {
			t.Errorf("Test %d: ServerSideEncryptionKeyMAC must be part of metadata: %v", i, mac)
		}
	}
}

var decryptObjectMetaTests = []struct {
	info    ObjectInfo
	request *http.Request
	expErr  error
}{
	{
		info:    ObjectInfo{Size: 100},
		request: &http.Request{Header: http.Header{}},
		expErr:  nil,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{crypto.MetaAlgorithm: crypto.InsecureSealAlgorithm}},
		request: &http.Request{Header: http.Header{xhttp.AmzServerSideEncryption: []string{xhttp.AmzEncryptionAES}}},
		expErr:  nil,
	},
	{
		info:    ObjectInfo{Size: 0, UserDefined: map[string]string{crypto.MetaAlgorithm: crypto.InsecureSealAlgorithm}},
		request: &http.Request{Header: http.Header{xhttp.AmzServerSideEncryption: []string{xhttp.AmzEncryptionAES}}},
		expErr:  nil,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{crypto.MetaSealedKeySSEC: "EAAfAAAAAAD7v1hQq3PFRUHsItalxmrJqrOq6FwnbXNarxOOpb8jTWONPPKyM3Gfjkjyj6NCf+aB/VpHCLCTBA=="}},
		request: &http.Request{Header: http.Header{}},
		expErr:  errEncryptedObject,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{}},
		request: &http.Request{Method: http.MethodGet, Header: http.Header{xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES}}},
		expErr:  errInvalidEncryptionParameters,
	},
	{
		info:    ObjectInfo{Size: 100, UserDefined: map[string]string{}},
		request: &http.Request{Method: http.MethodHead, Header: http.Header{xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES}}},
		expErr:  errInvalidEncryptionParameters,
	},
	{
		info:    ObjectInfo{Size: 31, UserDefined: map[string]string{crypto.MetaAlgorithm: crypto.InsecureSealAlgorithm}},
		request: &http.Request{Header: http.Header{xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{xhttp.AmzEncryptionAES}}},
		expErr:  errObjectTampered,
	},
}

func TestDecryptObjectInfo(t *testing.T) {
	for i, test := range decryptObjectMetaTests {
		if encrypted, err := DecryptObjectInfo(&test.info, test.request); err != test.expErr {
			t.Errorf("Test %d: Decryption returned wrong error code: got %d , want %d", i, err, test.expErr)
		} else if _, enc := crypto.IsEncrypted(test.info.UserDefined); encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is encrypted but it is not", i)
		} else if !encrypted && enc != encrypted {
			t.Errorf("Test %d: Decryption thinks object is not encrypted but it is", i)
		}
	}
}

var decryptETagTests = []struct {
	ObjectKey  crypto.ObjectKey
	ObjectInfo ObjectInfo
	ShouldFail bool
	ETag       string
}{
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "20000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d69354"},
		ETag:       "8ad3fe6b84bf38489e95c701c84355b6",
	},
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "20000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d6935"},
		ETag:       "",
		ShouldFail: true, // ETag is not a valid hex value
	},
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "00000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d69354"},
		ETag:       "",
		ShouldFail: true, // modified ETag
	},

	// Special tests for ETags that end with a '-x'
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "916516b396f0f4d4f2a0e7177557bec4-1"},
		ETag:       "916516b396f0f4d4f2a0e7177557bec4-1",
	},
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "916516b396f0f4d4f2a0e7177557bec4-738"},
		ETag:       "916516b396f0f4d4f2a0e7177557bec4-738",
	},
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "916516b396f0f4d4f2a0e7177557bec4-Q"},
		ETag:       "",
		ShouldFail: true, // Q is not a number
	},
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "16516b396f0f4d4f2a0e7177557bec4-1"},
		ETag:       "",
		ShouldFail: true, // ETag prefix is not a valid hex value
	},
	{
		ObjectKey:  [32]byte{},
		ObjectInfo: ObjectInfo{ETag: "16516b396f0f4d4f2a0e7177557bec4-1-2"},
		ETag:       "",
		ShouldFail: true, // ETag contains multiple: -
	},
}

func TestDecryptETag(t *testing.T) {
	for i, test := range decryptETagTests {
		etag, err := DecryptETag(test.ObjectKey, test.ObjectInfo)
		if err != nil && !test.ShouldFail {
			t.Fatalf("Test %d: should succeed but failed: %v", i, err)
		}
		if err == nil && test.ShouldFail {
			t.Fatalf("Test %d: should fail but succeeded", i)
		}
		if err == nil {
			if etag != test.ETag {
				t.Fatalf("Test %d: ETag mismatch: got %s - want %s", i, etag, test.ETag)
			}
		}
	}
}

// Tests for issue reproduced when getting the right encrypted
// offset of the object.
func TestGetDecryptedRange_Issue50(t *testing.T) {
	rs, err := parseRequestRangeSpec("bytes=594870256-594870263")
	if err != nil {
		t.Fatal(err)
	}

	objInfo := ObjectInfo{
		Bucket: "bucket",
		Name:   "object",
		Size:   595160760,
		UserDefined: map[string]string{
			crypto.MetaMultipart:                   "",
			crypto.MetaIV:                          "HTexa=",
			crypto.MetaAlgorithm:                   "DAREv2-HMAC-SHA256",
			crypto.MetaSealedKeySSEC:               "IAA8PGAA==",
			ReservedMetadataPrefix + "actual-size": "594870264",
			"content-type":                         "application/octet-stream",
			"etag":                                 "166b1545b4c1535294ee0686678bea8c-2",
		},
		Parts: []ObjectPartInfo{
			{
				Number:     1,
				Size:       297580380,
				ActualSize: 297435132,
			},
			{
				Number:     2,
				Size:       297580380,
				ActualSize: 297435132,
			},
		},
	}

	encOff, encLength, skipLen, seqNumber, partStart, err := objInfo.GetDecryptedRange(rs)
	if err != nil {
		t.Fatalf("Test: failed %s", err)
	}
	if encOff != 595127964 {
		t.Fatalf("Test: expected %d, got %d", 595127964, encOff)
	}
	if encLength != 32796 {
		t.Fatalf("Test: expected %d, got %d", 32796, encLength)
	}
	if skipLen != 32756 {
		t.Fatalf("Test: expected %d, got %d", 32756, skipLen)
	}
	if seqNumber != 4538 {
		t.Fatalf("Test: expected %d, got %d", 4538, seqNumber)
	}
	if partStart != 1 {
		t.Fatalf("Test: expected %d, got %d", 1, partStart)
	}
}

func TestGetDecryptedRange(t *testing.T) {
	var (
		pkgSz     = int64(64) * humanize.KiByte
		minPartSz = int64(5) * humanize.MiByte
		maxPartSz = int64(5) * humanize.GiByte

		getEncSize = func(s int64) int64 {
			v, _ := sio.EncryptedSize(uint64(s))
			return int64(v)
		}
		udMap = func(isMulti bool) map[string]string {
			m := map[string]string{
				crypto.MetaAlgorithm: crypto.InsecureSealAlgorithm,
				crypto.MetaMultipart: "1",
			}
			if !isMulti {
				delete(m, crypto.MetaMultipart)
			}
			return m
		}
	)

	// Single part object tests

	mkSPObj := func(s int64) ObjectInfo {
		return ObjectInfo{
			Size:        getEncSize(s),
			UserDefined: udMap(false),
		}
	}

	testSP := []struct {
		decSz int64
		oi    ObjectInfo
	}{
		{0, mkSPObj(0)},
		{1, mkSPObj(1)},
		{pkgSz - 1, mkSPObj(pkgSz - 1)},
		{pkgSz, mkSPObj(pkgSz)},
		{2*pkgSz - 1, mkSPObj(2*pkgSz - 1)},
		{minPartSz, mkSPObj(minPartSz)},
		{maxPartSz, mkSPObj(maxPartSz)},
	}

	for i, test := range testSP {
		{
			// nil range
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(nil)
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			if skip != 0 || sn != 0 || ps != 0 || o != 0 || l != getEncSize(test.decSz) {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		if test.decSz >= 10 {
			// first 10 bytes
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, 0, 9})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			rLen := pkgSz + 32
			if test.decSz < pkgSz {
				rLen = test.decSz + 32
			}
			if skip != 0 || sn != 0 || ps != 0 || o != 0 || l != rLen {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		kb32 := int64(32) * humanize.KiByte
		if test.decSz >= (64+32)*humanize.KiByte {
			// Skip the first 32Kib, and read the next 64Kib
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, kb32, 3*kb32 - 1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			rLen := (pkgSz + 32) * 2
			if test.decSz < 2*pkgSz {
				rLen = (pkgSz + 32) + (test.decSz - pkgSz + 32)
			}
			if skip != kb32 || sn != 0 || ps != 0 || o != 0 || l != rLen {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		if test.decSz >= (64*2+32)*humanize.KiByte {
			// Skip the first 96Kib and read the next 64Kib
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, 3 * kb32, 5*kb32 - 1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			rLen := (pkgSz + 32) * 2
			if test.decSz-pkgSz < 2*pkgSz {
				rLen = (pkgSz + 32) + (test.decSz - pkgSz + 32*2)
			}
			if skip != kb32 || sn != 1 || ps != 0 || o != pkgSz+32 || l != rLen {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}
	}

	// Multipart object tests
	var (
		// make a multipart object-info given part sizes
		mkMPObj = func(sizes []int64) ObjectInfo {
			r := make([]ObjectPartInfo, len(sizes))
			sum := int64(0)
			for i, s := range sizes {
				r[i].Number = i
				r[i].Size = getEncSize(s)
				sum += r[i].Size
			}
			return ObjectInfo{
				Size:        sum,
				UserDefined: udMap(true),
				Parts:       r,
			}
		}
		// Simple useful utilities
		repeat = func(k int64, n int) []int64 {
			a := []int64{}
			for range n {
				a = append(a, k)
			}
			return a
		}
		lsum = func(s []int64) int64 {
			sum := int64(0)
			for _, i := range s {
				if i < 0 {
					return -1
				}
				sum += i
			}
			return sum
		}
		esum = func(oi ObjectInfo) int64 {
			sum := int64(0)
			for _, i := range oi.Parts {
				sum += i.Size
			}
			return sum
		}
	)

	s1 := []int64{5487701, 5487799, 3}
	s2 := repeat(5487701, 5)
	s3 := repeat(maxPartSz, 10000)
	testMPs := []struct {
		decSizes []int64
		oi       ObjectInfo
	}{
		{s1, mkMPObj(s1)},
		{s2, mkMPObj(s2)},
		{s3, mkMPObj(s3)},
	}

	// This function is a reference (re-)implementation of
	// decrypted range computation, written solely for the purpose
	// of the unit tests.
	//
	// `s` gives the decrypted part sizes, and the other
	// parameters describe the desired read segment. When
	// `isFromEnd` is true, `skipLen` argument is ignored.
	decryptedRangeRef := func(s []int64, skipLen, readLen int64, isFromEnd bool) (o, l, skip int64, sn uint32, ps int) {
		oSize := lsum(s)
		if isFromEnd {
			skipLen = oSize - readLen
		}
		if skipLen < 0 || readLen < 0 || oSize < 0 || skipLen+readLen > oSize {
			t.Fatalf("Impossible read specified: %d %d %d", skipLen, readLen, oSize)
		}

		var cumulativeSum, cumulativeEncSum int64
		toRead := readLen
		readStart := false
		for i, v := range s {
			partOffset := int64(0)
			partDarePkgOffset := int64(0)
			if !readStart && cumulativeSum+v > skipLen {
				// Read starts at the current part
				readStart = true

				partOffset = skipLen - cumulativeSum

				// All return values except `l` are
				// calculated here.
				sn = uint32(partOffset / pkgSz)
				skip = partOffset % pkgSz
				ps = i
				o = cumulativeEncSum + int64(sn)*(pkgSz+32)

				partDarePkgOffset = partOffset - skip
			}
			if readStart {
				currentPartBytes := v - partOffset
				currentPartDareBytes := v - partDarePkgOffset
				if currentPartBytes < toRead {
					toRead -= currentPartBytes
					l += getEncSize(currentPartDareBytes)
				} else {
					// current part has the last
					// byte required
					lbPartOffset := partOffset + toRead - 1

					// round up the lbPartOffset
					// to the end of the
					// corresponding DARE package
					lbPkgEndOffset := min(lbPartOffset-(lbPartOffset%pkgSz)+pkgSz, v)
					bytesToDrop := v - lbPkgEndOffset

					// Last segment to update `l`
					l += getEncSize(currentPartDareBytes - bytesToDrop)
					break
				}
			}

			cumulativeSum += v
			cumulativeEncSum += getEncSize(v)
		}
		return o, l, skip, sn, ps
	}

	for i, test := range testMPs {
		{
			// nil range
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(nil)
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}
			if o != 0 || l != esum(test.oi) || skip != 0 || sn != 0 || ps != 0 {
				t.Errorf("Case %d: test failed: %d %d %d %d %d", i, o, l, skip, sn, ps)
			}
		}

		// Skip 1Mib and read 1Mib (in the decrypted object)
		//
		// The check below ensures the object is large enough
		// for the read.
		if lsum(test.decSizes) >= 2*humanize.MiByte {
			skipLen, readLen := int64(1)*humanize.MiByte, int64(1)*humanize.MiByte
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{false, skipLen, skipLen + readLen - 1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}

			oRef, lRef, skipRef, snRef, psRef := decryptedRangeRef(test.decSizes, skipLen, readLen, false)
			if o != oRef || l != lRef || skip != skipRef || sn != snRef || ps != psRef {
				t.Errorf("Case %d: test failed: %d %d %d %d %d (Ref: %d %d %d %d %d)",
					i, o, l, skip, sn, ps, oRef, lRef, skipRef, snRef, psRef)
			}
		}

		// Read the last 6Mib+1 bytes of the (decrypted)
		// object
		//
		// The check below ensures the object is large enough
		// for the read.
		readLen := int64(6)*humanize.MiByte + 1
		if lsum(test.decSizes) >= readLen {
			o, l, skip, sn, ps, err := test.oi.GetDecryptedRange(&HTTPRangeSpec{true, -readLen, -1})
			if err != nil {
				t.Errorf("Case %d: unexpected err: %v", i, err)
			}

			oRef, lRef, skipRef, snRef, psRef := decryptedRangeRef(test.decSizes, 0, readLen, true)
			if o != oRef || l != lRef || skip != skipRef || sn != snRef || ps != psRef {
				t.Errorf("Case %d: test failed: %d %d %d %d %d (Ref: %d %d %d %d %d)",
					i, o, l, skip, sn, ps, oRef, lRef, skipRef, snRef, psRef)
			}
		}
	}
}

var getDefaultOptsTests = []struct {
	headers        http.Header
	copySource     bool
	metadata       map[string]string
	encryptionType encrypt.Type
	err            error
}{
	{
		headers: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{"AES256"},
			xhttp.AmzServerSideEncryptionCustomerKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		copySource:     false,
		metadata:       nil,
		encryptionType: encrypt.SSEC,
		err:            nil,
	}, // 0
	{
		headers: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{"AES256"},
			xhttp.AmzServerSideEncryptionCustomerKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		copySource:     true,
		metadata:       nil,
		encryptionType: "",
		err:            nil,
	}, // 1
	{
		headers: http.Header{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: []string{"AES256"},
			xhttp.AmzServerSideEncryptionCustomerKey:       []string{"Mz"},
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		copySource:     false,
		metadata:       nil,
		encryptionType: "",
		err:            crypto.ErrInvalidCustomerKey,
	}, // 2
	{
		headers:        http.Header{xhttp.AmzServerSideEncryption: []string{"AES256"}},
		copySource:     false,
		metadata:       nil,
		encryptionType: encrypt.S3,
		err:            nil,
	}, // 3
	{
		headers:    http.Header{},
		copySource: false,
		metadata: map[string]string{
			crypto.MetaSealedKeyS3:       base64.StdEncoding.EncodeToString(make([]byte, 64)),
			crypto.MetaKeyID:             "kms-key",
			crypto.MetaDataEncryptionKey: "m-key",
		},
		encryptionType: encrypt.S3,
		err:            nil,
	}, // 4
	{
		headers:    http.Header{},
		copySource: true,
		metadata: map[string]string{
			crypto.MetaSealedKeyS3:       base64.StdEncoding.EncodeToString(make([]byte, 64)),
			crypto.MetaKeyID:             "kms-key",
			crypto.MetaDataEncryptionKey: "m-key",
		},
		encryptionType: "",
		err:            nil,
	}, // 5
	{
		headers: http.Header{
			xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm: []string{"AES256"},
			xhttp.AmzServerSideEncryptionCopyCustomerKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		copySource:     true,
		metadata:       nil,
		encryptionType: encrypt.SSEC,
		err:            nil,
	}, // 6
	{
		headers: http.Header{
			xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm: []string{"AES256"},
			xhttp.AmzServerSideEncryptionCopyCustomerKey:       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5:    []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		copySource:     false,
		metadata:       nil,
		encryptionType: "",
		err:            nil,
	}, // 7
}

func TestGetDefaultOpts(t *testing.T) {
	for i, test := range getDefaultOptsTests {
		opts, err := getDefaultOpts(test.headers, test.copySource, test.metadata)
		if test.err != err {
			t.Errorf("Case %d: expected err: %v , actual err: %v", i, test.err, err)
		}
		if err == nil {
			if opts.ServerSideEncryption == nil && test.encryptionType != "" {
				t.Errorf("Case %d: expected opts to be of %v encryption type", i, test.encryptionType)
			}
			if opts.ServerSideEncryption != nil && test.encryptionType != opts.ServerSideEncryption.Type() {
				t.Errorf("Case %d: expected opts to have encryption type %v but was %v ", i, test.encryptionType, opts.ServerSideEncryption.Type())
			}
		}
	}
}
