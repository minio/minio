package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	signV4 "github.com/minio/minio/pkg/signature"
)

func isRequestJWT(r *http.Request) bool {
	if _, ok := r.Header["Authorization"]; ok {
		if strings.HasPrefix(r.Header.Get("Authorization"), jwtAlgorithm) {
			return ok
		}
	}
	return false
}

func isRequestSignatureV4(r *http.Request) bool {
	if _, ok := r.Header["Authorization"]; ok {
		if strings.HasPrefix(r.Header.Get("Authorization"), signV4Algorithm) {
			return ok
		}
	}
	return false
}

func isRequestPresignedSignatureV4(r *http.Request) bool {
	if _, ok := r.URL.Query()["X-Amz-Credential"]; ok {
		return ok
	}
	return false
}

func isRequestPostPolicySignatureV4(r *http.Request) bool {
	if _, ok := r.Header["Content-Type"]; ok {
		if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
			return true
		}
	}
	return false
}

func isRequestRequiresACLCheck(r *http.Request) bool {
	if isRequestSignatureV4(r) || isRequestPresignedSignatureV4(r) || isRequestPostPolicySignatureV4(r) {
		return false
	}
	return true
}

func isSignV4ReqAuthenticated(sign *signV4.Signature, r *http.Request) bool {
	auth := sign.SetHTTPRequestToVerify(r)
	if isRequestSignatureV4(r) {
		dummyPayload := sha256.Sum256([]byte(""))
		ok, err := auth.DoesSignatureMatch(hex.EncodeToString(dummyPayload[:]))
		if err != nil {
			errorIf(err.Trace(), "Signature verification failed.", nil)
			return false
		}
		return ok
	} else if isRequestPresignedSignatureV4(r) {
		ok, err := auth.DoesPresignedSignatureMatch()
		if err != nil {
			errorIf(err.Trace(), "Presigned signature verification failed.", nil)
			return false
		}
		return ok
	}
	return false
}
