package signers

//import (
//	"bytes"
//	"crypto/hmac"
//	"crypto/sha1"
//	"encoding/base64"
//	"fmt"
//	"io"
//	"net/http"
//	"net/url"
//	"sort"
//	"strings"
//
//	"github.com/minio-io/minio/pkg/utils/database"
//)
//
//func ValidateAccessKey(key []byte) bool {
//	for _, char := range key {
//		if isalnum(char) {
//			continue
//		}
//		switch char {
//		case '-':
//		case '.':
//		case '_':
//		case '~':
//			continue
//		default:
//			return false
//		}
//	}
//	return true
//}
//
//func getAccessID() {
//}
//
//func getSecretID() {
//}
//
//// This package implements verification side of Object API Signature request
//func ValidateRequest(req *http.Request) (bool, error) {
//	if date := req.Header.Get("Date"); date == "" {
//		return false, fmt.Errorf("Date should be set")
//	}
//	hm := hmac.New(sha1.New, []byte(SecretAccessKey))
//	ss := getStringToSign(req)
//	io.WriteString(hm, ss)
//	authHeader := new(bytes.Buffer)
//	if req.Header.Get("User-Agent") == "Minio" {
//		fmt.Fprintf(authHeader, "MINIO %s:", AccessKey)
//	} else {
//		fmt.Fprintf(authHeader, "AWS %s:", AccessKey)
//	}
//	encoder := base64.NewEncoder(base64.StdEncoding, authHeader)
//	encoder.Write(hm.Sum(nil))
//	defer encoder.Close()
//	if req.Header.Get("Authorization") != authHeader.String() {
//		return false, fmt.Errorf("Authorization header mismatch")
//	}
//	return true, nil
//}
//
//// From the Amazon docs:
////
//// StringToSign = HTTP-Verb + "\n" +
//// 	 Content-MD5 + "\n" +
////	 Content-Type + "\n" +
////	 Date + "\n" +
////	 CanonicalizedAmzHeaders +
////	 CanonicalizedResource;
//func getStringToSign(req *http.Request) string {
//	buf := new(bytes.Buffer)
//	buf.WriteString(req.Method)
//	buf.WriteByte('\n')
//	buf.WriteString(req.Header.Get("Content-MD5"))
//	buf.WriteByte('\n')
//	buf.WriteString(req.Header.Get("Content-Type"))
//	buf.WriteByte('\n')
//	if req.Header.Get("x-amz-date") == "" {
//		buf.WriteString(req.Header.Get("Date"))
//	}
//	buf.WriteByte('\n')
//	writeCanonicalizedAmzHeaders(buf, req)
//	writeCanonicalizedResource(buf, req)
//	return buf.String()
//}
//
//func hasPrefixCaseInsensitive(s, pfx string) bool {
//	if len(pfx) > len(s) {
//		return false
//	}
//	shead := s[:len(pfx)]
//	if shead == pfx {
//		return true
//	}
//	shead = strings.ToLower(shead)
//	return shead == pfx || shead == strings.ToLower(pfx)
//}
//
//func writeCanonicalizedAmzHeaders(buf *bytes.Buffer, req *http.Request) {
//	amzHeaders := make([]string, 0)
//	vals := make(map[string][]string)
//	for k, vv := range req.Header {
//		if hasPrefixCaseInsensitive(k, "x-amz-") {
//			lk := strings.ToLower(k)
//			amzHeaders = append(amzHeaders, lk)
//			vals[lk] = vv
//		}
//	}
//	sort.Strings(amzHeaders)
//	for _, k := range amzHeaders {
//		buf.WriteString(k)
//		buf.WriteByte(':')
//		for idx, v := range vals[k] {
//			if idx > 0 {
//				buf.WriteByte(',')
//			}
//			if strings.Contains(v, "\n") {
//				// TODO: "Unfold" long headers that
//				// span multiple lines (as allowed by
//				// RFC 2616, section 4.2) by replacing
//				// the folding white-space (including
//				// new-line) by a single space.
//				buf.WriteString(v)
//			} else {
//				buf.WriteString(v)
//			}
//		}
//		buf.WriteByte('\n')
//	}
//}
//
//// Must be sorted:
//var subResList = []string{"acl", "lifecycle", "location", "logging", "notification", "partNumber", "policy", "requestPayment", "torrent", "uploadId", "uploads", "versionId", "versioning", "versions", "website"}
//
//// From the Amazon docs:
////
//// CanonicalizedResource = [ "/" + Bucket ] +
//// 	  <HTTP-Request-URI, from the protocol name up to the query string> +
//// 	  [ sub-resource, if present. For example "?acl", "?location", "?logging", or "?torrent"];
//func writeCanonicalizedResource(buf *bytes.Buffer, req *http.Request) {
//	bucket := getBucketFromHostname(req)
//	if bucket != "" {
//		buf.WriteByte('/')
//		buf.WriteString(bucket)
//	}
//	buf.WriteString(req.URL.Path)
//	if req.URL.RawQuery != "" {
//		n := 0
//		vals, _ := url.ParseQuery(req.URL.RawQuery)
//		for _, subres := range subResList {
//			if vv, ok := vals[subres]; ok && len(vv) > 0 {
//				n++
//				if n == 1 {
//					buf.WriteByte('?')
//				} else {
//					buf.WriteByte('&')
//				}
//				buf.WriteString(subres)
//				if len(vv[0]) > 0 {
//					buf.WriteByte('=')
//					buf.WriteString(url.QueryEscape(vv[0]))
//				}
//			}
//		}
//	}
//}
//
//// hasDotSuffix reports whether s ends with "." + suffix.
//func hasDotSuffix(s string, suffix string) bool {
//	return len(s) >= len(suffix)+1 && strings.HasSuffix(s, suffix) && s[len(s)-len(suffix)-1] == '.'
//}
//
//func getBucketFromHostname(req *http.Request) string {
//	host := req.Host
//	if host == "" {
//		host = req.URL.Host
//	}
//	if host == "s3.amazonaws.com" {
//		return ""
//	}
//	if hostSuffix := "s3.amazonaws.com"; hasDotSuffix(host, hostSuffix) {
//		return host[:len(host)-len(hostSuffix)-1]
//	}
//	if lastColon := strings.LastIndex(host, ":"); lastColon != -1 {
//		return host[:lastColon]
//	}
//	return host
//}
