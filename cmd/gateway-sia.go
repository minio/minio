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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/NebulousLabs/Sia/api"
	"github.com/minio/minio-go/pkg/set"
)

type siaObjects struct {
	gatewayUnsupported
	Address  string // Address and port of Sia Daemon.
	CacheDir string // Temporary storage location for file transfers.
	RootDir  string // Root directory to store files on Sia.
	password string // Sia password for uploading content in authenticated manner.
}

// SiaServiceError is a custom error type used by Sia cache layer
type SiaServiceError struct {
	Code    string
	Message string
}

func (e SiaServiceError) Error() string {
	return fmt.Sprintf("Sia Error: %s",
		e.Message)
}

// Also: SiaErrorDaemon is a valid code
var siaErrorUnknown = &SiaServiceError{
	Code:    "SiaErrorUnknown",
	Message: "An unknown error has occurred.",
}

var siaErrorObjectDoesNotExistInBucket = &SiaServiceError{
	Code:    "SiaErrorObjectDoesNotExistInBucket",
	Message: "Object does not exist in bucket.",
}

var siaErrorObjectAlreadyExists = &SiaServiceError{
	Code:    "SiaErrorObjectAlreadyExists",
	Message: "Object already exists in bucket.",
}

var siaErrorInvalidObjectName = &SiaServiceError{
	Code:    "SiaErrorInvalidObjectName",
	Message: "Object name not suitable for Sia.",
}

var siaErrorNotImplemented = &SiaServiceError{
	Code:    "SiaErrorNotImplemented",
	Message: "Not Implemented",
}

// Convert Sia errors to minio object layer errors.
func siaToObjectError(err *SiaServiceError, params ...string) error {
	if err == nil {
		return nil
	}

	switch err.Code {
	case "SiaErrorUnknown":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorObjectDoesNotExistInBucket":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorObjectAlreadyExists":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorInvalidObjectName":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDaemon":
		return fmt.Errorf("Sia Daemon: %s", err.Message)
	default:
		return fmt.Errorf("Sia: %s", err.Message)
	}
}

// User-supplied password, cached.
var apiPassword string

// non2xx returns true for non-success HTTP status codes.
func non2xx(code int) bool {
	return code < 200 || code > 299
}

// decodeError returns the api.Error from a API response. This method should
// only be called if the response's status code is non-2xx. The error returned
// may not be of type api.Error in the event of an error unmarshalling the
// JSON.
func decodeError(resp *http.Response) error {
	var apiErr api.Error
	err := json.NewDecoder(resp.Body).Decode(&apiErr)
	if err != nil {
		return err
	}
	return apiErr
}

// apiGet wraps a GET request with a status code check, such that if the GET does
// not return 2xx, the error will be read and returned. The response body is
// not closed.
func apiGet(addr, call, apiPassword string) (*http.Response, error) {
	if host, port, _ := net.SplitHostPort(addr); host == "" {
		addr = net.JoinHostPort("localhost", port)
	}
	resp, err := api.HttpGETAuthenticated("http://"+addr+call, apiPassword)
	if err != nil {
		return nil, errors.New("no response from daemon")
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.New("API call not recognized: " + call)
	}
	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// apiPost wraps a POST request with a status code check, such that if the POST
// does not return 2xx, the error will be read and returned. The response body
// is not closed.
func apiPost(addr, call, vals, apiPassword string) (*http.Response, error) {
	if host, port, _ := net.SplitHostPort(addr); host == "" {
		addr = net.JoinHostPort("localhost", port)
	}

	resp, err := api.HttpPOSTAuthenticated("http://"+addr+call, vals, apiPassword)
	if err != nil {
		return nil, errors.New("no response from daemon")
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.New("API call not recognized: " + call)
	}

	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// post makes an API call and discards the response. An error is returned if
// the response status is not 2xx.
func post(addr, call, vals, apiPassword string) error {
	resp, err := apiPost(addr, call, vals, apiPassword)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// getAPI makes a GET API call and decodes the response. An error is returned
// if the response status is not 2xx.
func getAPI(addr string, call string, apiPassword string, obj interface{}) error {
	resp, err := apiGet(addr, call, apiPassword)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return errors.New("expecting a response, but API returned status code 204 No Content")
	}

	err = json.NewDecoder(resp.Body).Decode(obj)
	if err != nil {
		return err
	}
	return nil
}

// get makes an API call and discards the response. An error is returned if the
// response status is not 2xx.
func get(addr, call, apiPassword string) error {
	resp, err := apiGet(addr, call, apiPassword)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// newSiaGateway returns Sia gatewaylayer
func newSiaGateway() (GatewayLayer, error) {
	sia := &siaObjects{
		Address:  os.Getenv("SIA_DAEMON_ADDR"),
		CacheDir: os.Getenv("SIA_CACHE_DIR"),
		RootDir:  os.Getenv("SIA_ROOT_DIR"),
		password: os.Getenv("MINIO_SECRET_KEY"),
	}

	// If Address not provided on command line or ENV, default to:
	if sia.Address == "" {
		sia.Address = "127.0.0.1:9980"
	}

	fmt.Printf("\nSia Gateway Configuration:\n")
	fmt.Printf("  Sia Daemon API Address: %s\n", sia.Address)
	fmt.Printf("  Sia Root Directory: %s\n", sia.RootDir)
	fmt.Printf("  Sia Cache Directory: %s\n", sia.CacheDir)
	return sia, nil
}

// loadSiaEnv will attempt to load Sia config from ENV
func (s *siaObjects) loadSiaEnv() {
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown() error {
	return nil
}

// StorageInfo is not relevant to Sia backend.
func (s *siaObjects) StorageInfo() (si StorageInfo) {
	return si
}

// MakeBucket creates a new container on Sia backend.
func (s *siaObjects) MakeBucketWithLocation(bucket, location string) error {
	return nil
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, err error) {
	buckets, err := s.ListBuckets()
	if err != nil {
		return bi, err
	}
	for _, binfo := range buckets {
		if binfo.Name == bucket {
			return binfo, nil
		}
	}
	return bi, traceError(BucketNotFound{Bucket: bucket})
}

// ListBuckets will detect and return existing buckets on Sia.
func (s *siaObjects) ListBuckets() (buckets []BucketInfo, err error) {
	sObjs, serr := s.listRenterFiles("")
	if serr != nil {
		return buckets, serr
	}

	m := make(set.StringSet)

	var prefix string
	if s.RootDir != "" {
		prefix = s.RootDir + "/"
	}

	for _, sObj := range sObjs {
		if strings.HasPrefix(sObj.SiaPath, prefix) {
			siaObj := strings.TrimPrefix(sObj.SiaPath, prefix)
			idx := strings.Index(siaObj, "/")
			if idx > 0 {
				m.Add(siaObj[0:idx])
			}
		}
	}

	for _, bktName := range m.ToSlice() {
		buckets = append(buckets, BucketInfo{
			Name:    bktName,
			Created: timeSentinel,
		})
	}

	return buckets, nil
}

// DeleteBucket deletes a bucket on Sia
func (s *siaObjects) DeleteBucket(bucket string) error {
	return traceError(BucketNotEmpty{})
}

func (s *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	siaObjs, siaErr := s.listRenterFiles(bucket)
	if siaErr != nil {
		return loi, siaToObjectError(siaErr)
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	var root string
	if s.RootDir != "" {
		root = s.RootDir + "/"
	}

	for _, sObj := range siaObjs {
		name := strings.TrimPrefix(sObj.SiaPath, pathJoin(root, bucket, "/"))
		if strings.HasPrefix(name, prefix) {
			loi.Objects = append(loi.Objects, ObjectInfo{
				Bucket: bucket,
				Name:   name,
				Size:   int64(sObj.Filesize),
				IsDir:  false,
			})
		}
	}
	return loi, nil
}

func (s *siaObjects) GetObject(bucket string, object string, startOffset int64, length int64, writer io.Writer) error {
	if !s.isValidObjectName(object) {
		return traceError(ObjectNameInvalid{bucket, object})
	}
	dstFile := pathJoin(s.CacheDir, mustGetUUID())
	defer fsRemoveFile(dstFile)

	var siaObj = pathJoin(s.RootDir, bucket, object)
	if err := get(s.Address, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(dstFile), s.password); err != nil {
		return err
	}

	reader, size, err := fsOpenFile(dstFile, startOffset)
	if err != nil {
		return err
	}
	defer reader.Close()

	bufSize := int64(readSizeV1)
	if length > 0 && bufSize > length {
		bufSize = length
	}

	// For negative length we read everything.
	if length < 0 {
		length = size - startOffset
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > size || startOffset+length > size {
		return traceError(InvalidRange{startOffset, length, size})
	}

	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, length), buf)

	return err
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	var siaObj = pathJoin(s.RootDir, bucket, object)
	sObjs, serr := s.listRenterFiles(bucket)
	if serr != nil {
		return objInfo, serr
	}

	for _, sObj := range sObjs {
		if sObj.SiaPath == siaObj {
			// Metadata about sia objects is just quite minimal
			// there is nothing else sia provides other than size.
			return ObjectInfo{
				Bucket: bucket,
				Name:   object,
				Size:   int64(sObj.Filesize),
				IsDir:  false,
			}, nil
		}
	}

	return objInfo, traceError(ObjectNotFound{bucket, object})
}

func (s *siaObjects) isSiaFileAvailable(bucket string, object string) bool {
	var siaObj = pathJoin(s.RootDir, bucket, object)
	sObjs, serr := s.listRenterFiles(bucket)
	if serr != nil {
		return false
	}

	for _, sObj := range sObjs {
		if sObj.SiaPath == siaObj {
			// Object found
			return sObj.Available
		}
	}
	return false
}

func (s *siaObjects) waitTillSiaUploadCompletes(bucket string, object string) *SiaServiceError {
	for {
		if s.isSiaFileAvailable(bucket, object) {
			return nil
		}
		time.Sleep(3 * time.Second)
	}
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(bucket string, object string, data *HashReader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	// Check the object's name first
	if !s.isValidObjectName(object) {
		return objInfo, siaToObjectError(siaErrorInvalidObjectName, bucket, object)
	}

	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}
	buf := make([]byte, int(bufSize))

	srcFile := pathJoin(s.CacheDir, mustGetUUID())
	defer fsRemoveFile(srcFile)

	if _, err = fsCreateFile(srcFile, data, buf, data.Size()); err != nil {
		return objInfo, err
	}

	if err = data.Verify(); err != nil {
		return objInfo, err
	}

	var siaObj = pathJoin(s.RootDir, bucket, object)
	if err = post(s.Address, "/renter/upload/"+siaObj, "source="+srcFile, s.password); err != nil {
		return objInfo, err
	}

	// Need to wait for upload to complete
	s.waitTillSiaUploadCompletes(bucket, object)

	return objInfo, nil
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(bucket string, object string) error {
	// Tell Sia daemon to delete the object
	var siaObj = pathJoin(s.RootDir, bucket, object)
	return post(s.Address, "/renter/delete/"+siaObj, "", s.password)
}

// siaObjectInfo represents object info stored on Sia
type siaObjectInfo struct {
	SiaPath        string
	Filesize       uint64
	Renewing       bool
	Available      bool
	Redundancy     float64
	UploadProgress float64
}

// isValidObjectName returns whether or not the objectName provided is suitable for Sia
func (s *siaObjects) isValidObjectName(objectName string) bool {
	reg, _ := regexp.Compile("[^a-zA-Z0-9., _+-]+")
	return objectName == reg.ReplaceAllString(objectName, "")
}

// ListObjects will return a list of existing objects in the bucket provided
func (s *siaObjects) listRenterFiles(bucket string) (siaObjs []siaObjectInfo, e *SiaServiceError) {
	// Get list of all renter files
	var rf api.RenterFiles
	derr := getAPI(s.Address, "/renter/files", s.password, &rf)
	if derr != nil {
		return siaObjs, &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	var prefix string
	var root string
	if s.RootDir == "" {
		root = ""
	} else {
		root = s.RootDir + "/"
	}
	if bucket == "" {
		prefix = root
	} else {
		prefix = root + bucket + "/"
	}

	for _, f := range rf.Files {
		if strings.HasPrefix(f.SiaPath, prefix) {
			siaObjs = append(siaObjs, siaObjectInfo{
				SiaPath:        f.SiaPath,
				Filesize:       f.Filesize,
				Renewing:       f.Renewing,
				Available:      f.Available,
				Redundancy:     f.Redundancy,
				UploadProgress: f.UploadProgress,
			})
		}
	}
	return siaObjs, nil
}
