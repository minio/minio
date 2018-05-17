/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gorilla/mux"
)

var bucketNameRegexp = regexp.MustCompile("^[a-z0-9][a-z0-9\\.\\-]+[a-z0-9]$")

var labelRegexp = regexp.MustCompile("^[a-z0-9\\-]*[a-z0-9]$")

// ValidateBucketName - validates bucket name and returns error if it is invalid.
func ValidateBucketName(name string) error {
	// Bucket names cannot be no less than 3 and no more than 63 characters long.
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name %v must be at least 3 and no more than 255 characters long", name)
	}

	// Bucket names should be DNS name compatible.
	if !bucketNameRegexp.MatchString(name) {
		return fmt.Errorf("bucket name %s does not follow Amazon S3 standards", name)
	}

	// name must not be IPv4 address.
	if ip := net.ParseIP(name); ip != nil && ip.To4() != nil {
		return fmt.Errorf("bucket name %s does not follow Amazon S3 standards", name)
	}

	// Each label should be DNS name compatible.
	for _, label := range strings.Split(name, ".") {
		if strings.HasPrefix(label, "-") || !labelRegexp.MatchString(label) {
			return fmt.Errorf("bucket name %s does not follow Amazon S3 standards", name)
		}
	}

	return nil
}

var legacyBucketNameRegexp = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_\\.\\-]+[a-zA-Z0-9]$")

func validateCompatBucketName(name string) error {
	// IF minio is running in gateway mode and backend is s3, do legacy bucket name
	// validation to support very old existing bucket in the US East (N. Virginia)
	// Region. For more information refer
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
	if globalGatewayName == "s3" {
		if len(name) < 3 || len(name) > 255 {
			return fmt.Errorf("bucket name %v must be at least 3 and no more than 255 characters long", name)
		}

		if !legacyBucketNameRegexp.MatchString(name) {
			return fmt.Errorf("bucket name %s does not follow Amazon S3 standards", name)
		}

		return nil
	}

	return ValidateBucketName(name)
}

func validateS3ObjectKey(key string) error {
	if len(key) > 1024 {
		return fmt.Errorf("object key must be no more than 1024 characters long")
	}

	if !utf8.ValidString(key) {
		return fmt.Errorf("object key must be a valid UTF-8 string")
	}

	return nil
}

func validateObjectKey(key string) error {
	if err := validateS3ObjectKey(key); err != nil {
		return err
	}

	// Below are Minio specific checks.

	if key != "/" && strings.HasPrefix(key, "/") {
		return fmt.Errorf("object key %v must not start with / character", key)
	}

	if strings.Contains(key, `\`) {
		return fmt.Errorf(`object key %v must not contain \ character`, key)
	}

	if runtime.GOOS == "windows" {
		if strings.ContainsAny(key, `:*?"<>|`) {
			return fmt.Errorf(`object key %v must not contain :*?"<>| characters`, key)
		}
	}

	tokens := strings.Split(key, "/")
	for i, token := range tokens {
		// It is allowed to have last token as empty i.e. object key ending with `/` is allowed.
		if token == "" && i != len(tokens)-1 {
			return fmt.Errorf(`object key %v must not contain extra / character`, key)
		}

		// Minio does not support `.` and `..` because of underneath filesystem limitations.
		if token == "." || token == ".." {
			return fmt.Errorf(`object key %v must not contain '.' or '..' characters`, key)
		}

		// Only 255 long path segment is supported in GNU/Linux, Mac OSX and MS Windows.
		if len(token) > 255 {
			return fmt.Errorf("path segment %v in object key must be no more than 255 characters long", token)
		}
	}

	return nil
}

func getValidObjectKey(key string) (string, error) {
	// If minio backend is S3 in gateway mode, validate object key as per Amazon AWS S3.
	if globalGatewayName == "s3" {
		return key, validateS3ObjectKey(key)
	}

	// As MS Windows does not support case sensitivity, convert object key to lower case
	// only if minio is not running in gateway mode.
	if globalGatewayName == "" && runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}

	return key, validateObjectKey(key)
}

type requestArgs struct {
	vars        map[string]string // values from mux.Var().
	queryValues url.Values        // values from http.Request.URL.Query().
	header      http.Header       // values from http.Request.Header.
}

func (args requestArgs) CompatBucketName() (string, error) {
	bucketName := args.vars["bucket"]
	return bucketName, validateCompatBucketName(bucketName)
}

func (args requestArgs) BucketName() (string, error) {
	bucketName := args.vars["bucket"]
	return bucketName, ValidateBucketName(bucketName)
}

func (args requestArgs) ObjectName() (string, error) {
	objectName := args.vars["object"]

	// Even though this condition won't happen in gorilla mux router configuration,
	// Have the check for sanity.
	if objectName == "" {
		return "", fmt.Errorf("empty object name")
	}

	return getValidObjectKey(objectName)
}

func (args requestArgs) Prefix() (string, error) {
	// Prefix is as same as object key.
	prefix := args.queryValues.Get("prefix")

	return getValidObjectKey(prefix)
}

func (args requestArgs) StartAfter() (string, error) {
	// start-after is as same as object key.
	startAfter := args.queryValues.Get("start-after")

	return getValidObjectKey(startAfter)
}

func (args requestArgs) Marker() (string, error) {
	// marker is as same as object key.
	marker := args.queryValues.Get("marker")

	return getValidObjectKey(marker)
}

func (args requestArgs) KeyMarker() (string, error) {
	// key-marker is as same as object key.
	keyMarker := args.queryValues.Get("key-marker")

	return getValidObjectKey(keyMarker)
}

func (args requestArgs) CopySource() (string, string, error) {
	if _, found := args.header["X-Amz-Copy-Source"]; !found {
		return "", "", fmt.Errorf("x-amz-copy-source header is missing")
	}

	copySource := args.header.Get("X-Amz-Copy-Source")
	if unescapedCopySource, err := url.QueryUnescape(copySource); err == nil {
		copySource = unescapedCopySource
	}

	// Trim / prefix in copy source.
	// srcbucket/srcobject and /srcbucket/srcobject are supported in AWS S3.
	copySource = strings.TrimPrefix(copySource, "/")

	tokens := strings.SplitN(copySource, "/", 2)
	if len(tokens) != 2 {
		return "", "", fmt.Errorf("invalid copy source %v", copySource)
	}

	bucketName := tokens[0]
	if err := validateCompatBucketName(bucketName); err != nil {
		return "", "", err
	}

	if tokens[1] == "" {
		return "", "", fmt.Errorf("empty object name")
	}

	objectName, err := getValidObjectKey(tokens[1])

	return bucketName, objectName, err
}

func (args requestArgs) MetadataDirective() (string, error) {
	// COPY is default metadata directive.
	metadataDirective := "COPY"
	if _, found := args.header["X-Amz-Metadata-Directive"]; found {
		// x-amz-metadata-directive must be either COPY or REPLACE.
		metadataDirective = args.header.Get("X-Amz-Metadata-Directive")
		if metadataDirective != "COPY" && metadataDirective != "REPLACE" {
			return "", fmt.Errorf("invalid value %v in x-amz-metadata-directive", metadataDirective)
		}
	}

	return metadataDirective, nil
}

func (args requestArgs) UploadID() (string, error) {
	uploadID := args.queryValues.Get("uploadId")
	if uploadID == "" {
		return "", fmt.Errorf("empty upload ID")
	}

	return uploadID, nil
}

func (args requestArgs) PartNumber() (int, error) {
	s := args.queryValues.Get("partNumber")
	partNumber, err := strconv.Atoi(s)
	if err != nil || partNumber < 1 {
		return 0, fmt.Errorf("invalid part number %v", s)
	}

	return partNumber, nil
}

func (args requestArgs) PartNumberMarker() (int, error) {
	s := args.queryValues.Get("part-number-marker")
	if s == "" {
		return 0, nil
	}

	partNumberMarker, err := strconv.Atoi(s)
	if err != nil || partNumberMarker < 1 || partNumberMarker > globalMaxPartID {
		return 0, fmt.Errorf("invalid part number marker %v", s)
	}

	return partNumberMarker, nil
}

func (args requestArgs) ContinuationToken() string {
	return args.queryValues.Get("continuation-token")
}

func (args requestArgs) FetchOwner() bool {
	return args.queryValues.Get("fetch-owner") == "true"
}

func (args requestArgs) UploadIDMarker() string {
	return args.queryValues.Get("upload-id-marker")
}

func (args requestArgs) Delimiter() (string, error) {
	delimiter := args.queryValues.Get("delimiter")

	// IF minio is running in gateway mode and backend is s3, keep delimiter as is.
	if globalGatewayName == "s3" {
		return delimiter, nil
	}

	if delimiter != "" && delimiter != "/" {
		return "", fmt.Errorf("unsupported delimiter %v", delimiter)
	}

	return delimiter, nil
}

func (args requestArgs) getMaxList(key string) (int, error) {
	// If key is missing, use 1000 as default as per S3 specifications.
	s := args.queryValues.Get(key)
	if s == "" {
		return 1000, nil
	}

	maxCount, err := strconv.Atoi(s)
	if err != nil || maxCount < 0 {
		return 0, fmt.Errorf("invalid %v value %v", key, s)
	}

	if maxCount == 0 || maxCount > 1000 {
		return 1000, nil
	}

	return maxCount, nil
}

func (args requestArgs) MaxParts() (int, error) {
	return args.getMaxList("max-parts")
}

func (args requestArgs) MaxKeys() (int, error) {
	return args.getMaxList("max-keys")
}

func (args requestArgs) MaxUploads() (int, error) {
	return args.getMaxList("max-uploads")
}

func newRequestArgs(r *http.Request) requestArgs {
	return requestArgs{
		vars:        mux.Vars(r),
		queryValues: r.URL.Query(),
		header:      r.Header,
	}
}
