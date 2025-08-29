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
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	xxml "github.com/minio/xxml"
)

// Returns a hexadecimal representation of time at the
// time response is sent to the client.
func mustGetRequestID(t time.Time) string {
	return fmt.Sprintf("%X", t.UnixNano())
}

// setEventStreamHeaders to allow proxies to avoid buffering proxy responses
func setEventStreamHeaders(w http.ResponseWriter) {
	w.Header().Set(xhttp.ContentType, "text/event-stream")
	w.Header().Set(xhttp.CacheControl, "no-cache") // nginx to turn off buffering
	w.Header().Set("X-Accel-Buffering", "no")      // nginx to turn off buffering
}

// Write http common headers
func setCommonHeaders(w http.ResponseWriter) {
	// Set the "Server" http header.
	w.Header().Set(xhttp.ServerInfo, MinioStoreName)

	// Set `x-amz-bucket-region` only if region is set on the server
	// by default minio uses an empty region.
	if region := globalSite.Region(); region != "" {
		w.Header().Set(xhttp.AmzBucketRegion, region)
	}
	w.Header().Set(xhttp.AcceptRanges, "bytes")

	// Remove sensitive information
	crypto.RemoveSensitiveHeaders(w.Header())
}

// Encodes the response headers into XML format.
func encodeResponse(response any) []byte {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	if err := xml.NewEncoder(&buf).Encode(response); err != nil {
		bugLogIf(GlobalContext, err)
		return nil
	}
	return buf.Bytes()
}

// Use this encodeResponseList() to support control characters
// this function must be used by only ListObjects() for objects
// with control characters, this is a specialized extension
// to support AWS S3 compatible behavior.
//
// Do not use this function for anything other than ListObjects()
// variants, please open a github discussion if you wish to use
// this in other places.
func encodeResponseList(response any) []byte {
	var buf bytes.Buffer
	buf.WriteString(xxml.Header)
	if err := xxml.NewEncoder(&buf).Encode(response); err != nil {
		bugLogIf(GlobalContext, err)
		return nil
	}
	return buf.Bytes()
}

// Encodes the response headers into JSON format.
func encodeResponseJSON(response any) []byte {
	var bytesBuffer bytes.Buffer
	e := json.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Write parts count
func setPartsCountHeaders(w http.ResponseWriter, objInfo ObjectInfo) {
	if strings.Contains(objInfo.ETag, "-") && len(objInfo.Parts) > 0 {
		w.Header()[xhttp.AmzMpPartsCount] = []string{strconv.Itoa(len(objInfo.Parts))}
	}
}

// Write object header
func setObjectHeaders(ctx context.Context, w http.ResponseWriter, objInfo ObjectInfo, rs *HTTPRangeSpec, opts ObjectOptions) (err error) {
	// set common headers
	setCommonHeaders(w)

	// Set last modified time.
	lastModified := objInfo.ModTime.UTC().Format(http.TimeFormat)
	w.Header().Set(xhttp.LastModified, lastModified)

	// Set Etag if available.
	if objInfo.ETag != "" {
		w.Header()[xhttp.ETag] = []string{"\"" + objInfo.ETag + "\""}
	}

	if objInfo.ContentType != "" {
		w.Header().Set(xhttp.ContentType, objInfo.ContentType)
	}

	if objInfo.ContentEncoding != "" {
		w.Header().Set(xhttp.ContentEncoding, objInfo.ContentEncoding)
	}

	if !objInfo.Expires.IsZero() {
		w.Header().Set(xhttp.Expires, objInfo.Expires.UTC().Format(http.TimeFormat))
	}

	// Set tag count if object has tags
	if len(objInfo.UserTags) > 0 {
		tags, _ := tags.ParseObjectTags(objInfo.UserTags)
		if tags != nil && tags.Count() > 0 {
			w.Header()[xhttp.AmzTagCount] = []string{strconv.Itoa(tags.Count())}
			if opts.Tagging {
				// This is MinIO only extension to return back tags along with the count.
				w.Header()[xhttp.AmzObjectTagging] = []string{objInfo.UserTags}
			}
		}
	}

	// Set all other user defined metadata.
	for k, v := range objInfo.UserDefined {
		// Empty values for object lock and retention can be skipped.
		if v == "" && equals(k, xhttp.AmzObjectLockMode, xhttp.AmzObjectLockRetainUntilDate) {
			continue
		}

		if stringsHasPrefixFold(k, ReservedMetadataPrefixLower) {
			// Do not need to send any internal metadata
			// values to client.
			continue
		}

		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}

		var isSet bool
		for _, userMetadataPrefix := range userMetadataKeyPrefixes {
			if !stringsHasPrefixFold(k, userMetadataPrefix) {
				continue
			}
			// check the doc https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
			// For metadata values like "ö", "ÄMÄZÕÑ S3", and "öha, das sollte eigentlich
			// funktionieren", tested against a real AWS S3 bucket, S3 may encode incorrectly. For
			// example, "ö" was encoded as =?UTF-8?B?w4PCtg==?=, producing invalid UTF-8 instead
			// of =?UTF-8?B?w7Y=?=. This mirrors errors like the ä½ in another string.
			//
			// S3 uses B-encoding (Base64) for non-ASCII-heavy metadata and Q-encoding
			// (quoted-printable) for mostly ASCII strings. Long strings are split at word
			// boundaries to fit RFC 2047’s 75-character limit, ensuring HTTP parser
			// compatibility.
			//
			// However, this splitting increases header size and can introduce errors, unlike Go’s
			// mime package in MinIO, which correctly encodes strings with fixed B/Q encodings,
			// avoiding S3’s heuristic-driven issues.
			//
			// For MinIO developers, decode S3 metadata with mime.WordDecoder, validate outputs,
			// report encoding bugs to AWS, and use ASCII-only metadata to ensure reliable S3 API
			// compatibility.
			if needsMimeEncoding(v) {
				// see https://github.com/golang/go/blob/release-branch.go1.24/src/net/mail/message.go#L325
				if strings.ContainsAny(v, "\"#$%&'(),.:;<>@[]^`{|}~") {
					v = mime.BEncoding.Encode("UTF-8", v)
				} else {
					v = mime.QEncoding.Encode("UTF-8", v)
				}
			}
			w.Header()[strings.ToLower(k)] = []string{v}
			isSet = true
			break
		}

		if !isSet {
			w.Header().Set(k, v)
		}
	}

	var start, rangeLen int64
	totalObjectSize, err := objInfo.GetActualSize()
	if err != nil {
		return err
	}

	if rs == nil && opts.PartNumber > 0 {
		rs = partNumberToRangeSpec(objInfo, opts.PartNumber)
	}

	// For providing ranged content
	start, rangeLen, err = rs.GetOffsetLength(totalObjectSize)
	if err != nil {
		return err
	}

	// Set content length.
	w.Header().Set(xhttp.ContentLength, strconv.FormatInt(rangeLen, 10))
	if rs != nil {
		contentRange := fmt.Sprintf("bytes %d-%d/%d", start, start+rangeLen-1, totalObjectSize)
		w.Header().Set(xhttp.ContentRange, contentRange)
	}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" && objInfo.VersionID != nullVersionID {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	if objInfo.ReplicationStatus.String() != "" {
		w.Header()[xhttp.AmzBucketReplicationStatus] = []string{objInfo.ReplicationStatus.String()}
	}

	if objInfo.IsRemote() {
		// Check if object is being restored. For more information on x-amz-restore header see
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_ResponseSyntax
		w.Header()[xhttp.AmzStorageClass] = []string{filterStorageClass(ctx, objInfo.TransitionedObject.Tier)}
	}

	if lc, err := globalLifecycleSys.Get(objInfo.Bucket); err == nil {
		lc.SetPredictionHeaders(w, objInfo.ToLifecycleOpts())
	}

	if v, ok := objInfo.UserDefined[ReservedMetadataPrefix+"compression"]; ok {
		if i := strings.LastIndexByte(v, '/'); i >= 0 {
			v = v[i+1:]
		}
		w.Header()[xhttp.MinIOCompressed] = []string{v}
	}

	return nil
}

// needsEncoding reports whether s contains any bytes that need to be encoded.
// see mime.needsEncoding
func needsMimeEncoding(s string) bool {
	for _, b := range s {
		if (b < ' ' || b > '~') && b != '\t' {
			return true
		}
	}
	return false
}
