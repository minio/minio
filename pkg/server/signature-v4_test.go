/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	. "github.com/minio/minio/internal/gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

const (
	authHeader    = "AWS4-HMAC-SHA256"
	iso8601Format = "20060102T150405Z"
	yyyymmdd      = "20060102"
)

///
/// Excerpts from @lsegal - https://github.com/aws/aws-sdk-js/issues/659#issuecomment-120477258
///
///  User-Agent:
///
///      This is ignored from signing because signing this causes problems with generating pre-signed URLs
///      (that are executed by other agents) or when customers pass requests through proxies, which may
///      modify the user-agent.
///
///  Content-Length:
///
///      This is ignored from signing because generating a pre-signed URL should not provide a content-length
///      constraint, specifically when vending a S3 pre-signed PUT URL. The corollary to this is that when
///      sending regular requests (non-pre-signed), the signature contains a checksum of the body, which
///      implicitly validates the payload length (since changing the number of bytes would change the checksum)
///      and therefore this header is not valuable in the signature.
///
///  Content-Type:
///
///      Signing this header causes quite a number of problems in browser environments, where browsers
///      like to modify and normalize the content-type header in different ways. There is more information
///      on this in https://github.com/aws/aws-sdk-js/issues/244. Avoiding this field simplifies logic
///      and reduces the possibility of future bugs
///
///  Authorization:
///
///      Is skipped for obvious reasons
///
var ignoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Type":   true,
	"Content-Length": true,
	"User-Agent":     true,
}

// urlEncodedName encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func urlEncodeName(name string) (string, error) {
	// if object matches reserved string, no need to encode them
	reservedNames := regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
	if reservedNames.MatchString(name) {
		return name, nil
	}
	var encodedName string
	for _, s := range name {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				return "", errors.New("invalid utf-8")
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedName = encodedName + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedName, nil
}

// sum256Reader calculate sha256 sum for an input read seeker
func sum256Reader(reader io.ReadSeeker) ([]byte, error) {
	h := sha256.New()
	var err error

	start, _ := reader.Seek(0, 1)
	defer reader.Seek(start, 0)

	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		h.Write(byteBuffer)
	}

	if err != io.EOF {
		return nil, err
	}

	return h.Sum(nil), nil
}

// sum256 calculate sha256 sum for an input byte array
func sum256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// sumHMAC calculate hmac between two input byte array
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

func (s *MyAPISignatureV4Suite) newRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
	t := time.Now().UTC()
	req, err := http.NewRequest(method, urlStr, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("x-amz-date", t.Format(iso8601Format))
	if method == "" {
		method = "POST"
	}

	// add Content-Length
	req.ContentLength = contentLength

	// add body
	switch {
	case body == nil:
		req.Body = nil
	default:
		req.Body = ioutil.NopCloser(body)
	}

	// save for subsequent use
	hash := func() string {
		switch {
		case body == nil:
			return hex.EncodeToString(sum256([]byte{}))
		default:
			sum256Bytes, _ := sum256Reader(body)
			return hex.EncodeToString(sum256Bytes)
		}
	}
	hashedPayload := hash()
	req.Header.Set("x-amz-content-sha256", hashedPayload)

	var headers []string
	vals := make(map[string][]string)
	for k, vv := range req.Header {
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; ok {
			continue // ignored header
		}
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	headers = append(headers, "host")
	sort.Strings(headers)

	var canonicalHeaders bytes.Buffer
	for _, k := range headers {
		canonicalHeaders.WriteString(k)
		canonicalHeaders.WriteByte(':')
		switch {
		case k == "host":
			canonicalHeaders.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					canonicalHeaders.WriteByte(',')
				}
				canonicalHeaders.WriteString(v)
			}
			canonicalHeaders.WriteByte('\n')
		}
	}

	signedHeaders := strings.Join(headers, ";")

	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)
	encodedPath, _ := urlEncodeName(req.URL.Path)
	// convert any space strings back to "+"
	encodedPath = strings.Replace(encodedPath, "+", "%20", -1)

	//
	// canonicalRequest =
	//  <HTTPMethod>\n
	//  <CanonicalURI>\n
	//  <CanonicalQueryString>\n
	//  <CanonicalHeaders>\n
	//  <SignedHeaders>\n
	//  <HashedPayload>
	//
	canonicalRequest := strings.Join([]string{
		req.Method,
		encodedPath,
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		hashedPayload,
	}, "\n")

	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		"milkyway",
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := authHeader + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))

	date := sumHMAC([]byte("AWS4"+s.secretAccessKey), []byte(t.Format(yyyymmdd)))
	region := sumHMAC(date, []byte("milkyway"))
	service := sumHMAC(region, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		authHeader + " Credential=" + s.accessKeyID + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return req, nil
}
