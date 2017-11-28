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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/sha256-simd"
)

const (
	siaBackend = "sia"
)

type siaObjects struct {
	gatewayUnsupported
	Address  string // Address and port of Sia Daemon.
	TempDir  string // Temporary storage location for file transfers.
	RootDir  string // Root directory to store files on Sia.
	password string // Sia password for uploading content in authenticated manner.
}

func init() {
	const siaGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [SIA_DAEMON_ADDR]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENVIRONMENT VARIABLES: (Default values in parenthesis)
  ACCESS:
     MINIO_ACCESS_KEY: Custom access key (Do not reuse same access keys on all instances)
     MINIO_SECRET_KEY: Custom secret key (Do not reuse same secret keys on all instances)

  SIA_TEMP_DIR:        The name of the local Sia temporary storage directory. (.sia_temp)
  SIA_API_PASSWORD:    API password for Sia daemon. (default is empty)

EXAMPLES:
  1. Start minio gateway server for Sia backend.
      $ {{.HelpName}}

`

	MustRegisterGatewayCommand(cli.Command{
		Name:               siaBackend,
		Usage:              "Sia Decentralized Cloud.",
		Action:             siaGatewayMain,
		CustomHelpTemplate: siaGatewayTemplate,
		Flags:              append(serverFlags, globalFlags...),
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway sia' command line.
func siaGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	host := ctx.Args().First()
	// Validate gateway arguments.
	fatalIf(validateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	startGateway(ctx, &SiaGateway{host})
}

// SiaGateway implements Gateway.
type SiaGateway struct {
	host string // Sia daemon host address
}

// Name implements Gateway interface.
func (g *SiaGateway) Name() string {
	return siaBackend
}

// NewGatewayLayer returns b2 gateway layer, implements GatewayLayer interface to
// talk to B2 remote backend.
func (g *SiaGateway) NewGatewayLayer() (GatewayLayer, error) {
	log.Println(colorYellow("\n               *** Warning: Not Ready for Production ***"))
	return newSiaGatewayLayer(g.host)
}

// non2xx returns true for non-success HTTP status codes.
func non2xx(code int) bool {
	return code < 200 || code > 299
}

// decodeError returns the api.Error from a API response. This method should
// only be called if the response's status code is non-2xx. The error returned
// may not be of type api.Error in the event of an error unmarshalling the
// JSON.
type siaError struct {
	// Message describes the error in English. Typically it is set to
	// `err.Error()`. This field is required.
	Message string `json:"message"`
}

func (s siaError) Error() string {
	return s.Message
}

func decodeError(resp *http.Response) error {
	// Error is a type that is encoded as JSON and returned in an API response in
	// the event of an error. Only the Message field is required. More fields may
	// be added to this struct in the future for better error reporting.
	var apiErr siaError
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		return err
	}
	return apiErr
}

// SiaMethodNotSupported - returned if call returned error.
type SiaMethodNotSupported struct {
	method string
}

func (s SiaMethodNotSupported) Error() string {
	return fmt.Sprintf("API call not recognized: %s", s.method)
}

// apiGet wraps a GET request with a status code check, such that if the GET does
// not return 2xx, the error will be read and returned. The response body is
// not closed.
func apiGet(addr, call, apiPassword string) (*http.Response, error) {
	req, err := http.NewRequest("GET", "http://"+addr+call, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	if apiPassword != "" {
		req.SetBasicAuth("", apiPassword)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, SiaMethodNotSupported{call}
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
	req, err := http.NewRequest("POST", "http://"+addr+call, strings.NewReader(vals))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if apiPassword != "" {
		req.SetBasicAuth("", apiPassword)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, SiaMethodNotSupported{call}
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

// list makes a lists all the uploaded files, decodes the json response.
func list(addr string, apiPassword string, obj *renterFiles) error {
	resp, err := apiGet(addr, "/renter/files", apiPassword)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return fmt.Errorf("Expecting a response, but API returned %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(obj)
}

// get makes an API call and discards the response. An error is returned if the
// responsee status is not 2xx.
func get(addr, call, apiPassword string) error {
	resp, err := apiGet(addr, call, apiPassword)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// newSiaGatewayLayer returns Sia gatewaylayer
func newSiaGatewayLayer(host string) (GatewayLayer, error) {
	sia := &siaObjects{
		Address: host,
		// RootDir uses access key directly, provides partitioning for
		// concurrent users talking to same sia daemon.
		RootDir:  os.Getenv("MINIO_ACCESS_KEY"),
		TempDir:  os.Getenv("SIA_TEMP_DIR"),
		password: os.Getenv("SIA_API_PASSWORD"),
	}

	// If Address not provided on command line or ENV, default to:
	if sia.Address == "" {
		sia.Address = "127.0.0.1:9980"
	}

	// If local Sia temp directory not specified, default to:
	if sia.TempDir == "" {
		sia.TempDir = ".sia_temp"
	}

	var err error
	sia.TempDir, err = filepath.Abs(sia.TempDir)
	if err != nil {
		return nil, err
	}

	// Create the temp directory with proper permissions.
	// Ignore error when dir already exists.
	if err = os.MkdirAll(sia.TempDir, 0700); err != nil {
		return nil, err
	}

	log.Println(colorBlue("\nSia Gateway Configuration:"))
	log.Println(colorBlue("  Sia Daemon API Address:") + colorBold(fmt.Sprintf(" %s\n", sia.Address)))
	log.Println(colorBlue("  Sia Temp Directory:") + colorBold(fmt.Sprintf(" %s\n", sia.TempDir)))
	return sia, nil
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
	srcFile := pathJoin(s.TempDir, mustGetUUID())
	defer fsRemoveFile(srcFile)

	if _, err := fsCreateFile(srcFile, bytes.NewReader([]byte("")), nil, 0); err != nil {
		return err
	}

	sha256sum := sha256.Sum256([]byte(bucket))
	var siaObj = pathJoin(s.RootDir, bucket, hex.EncodeToString(sha256sum[:]))
	return post(s.Address, "/renter/upload/"+siaObj, "source="+srcFile, s.password)
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, err error) {
	sha256sum := sha256.Sum256([]byte(bucket))
	var siaObj = pathJoin(s.RootDir, bucket, hex.EncodeToString(sha256sum[:]))

	dstFile := pathJoin(s.TempDir, mustGetUUID())
	defer fsRemoveFile(dstFile)

	if err := get(s.Address, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(dstFile), s.password); err != nil {
		return bi, err
	}
	return BucketInfo{Name: bucket}, nil
}

// ListBuckets will detect and return existing buckets on Sia.
func (s *siaObjects) ListBuckets() (buckets []BucketInfo, err error) {
	sObjs, serr := s.listRenterFiles("")
	if serr != nil {
		return buckets, serr
	}

	m := make(set.StringSet)

	prefix := s.RootDir + "/"
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
			Created: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		})
	}

	return buckets, nil
}

// DeleteBucket deletes a bucket on Sia.
func (s *siaObjects) DeleteBucket(bucket string) error {
	sha256sum := sha256.Sum256([]byte(bucket))
	var siaObj = pathJoin(s.RootDir, bucket, hex.EncodeToString(sha256sum[:]))

	return post(s.Address, "/renter/delete/"+siaObj, "", s.password)
}

func (s *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	siaObjs, siaErr := s.listRenterFiles(bucket)
	if siaErr != nil {
		return loi, siaErr
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	root := s.RootDir + "/"

	sha256sum := sha256.Sum256([]byte(bucket))
	// FIXME(harsha) - No paginated output supported for Sia backend right now, only prefix
	// based filtering. Once list renter files API supports paginated output we can support
	// paginated results here as well - until then Listing is an expensive operation.
	for _, sObj := range siaObjs {
		name := strings.TrimPrefix(sObj.SiaPath, pathJoin(root, bucket, "/"))
		// Skip the file created specially when bucket was created.
		if name == hex.EncodeToString(sha256sum[:]) {
			continue
		}
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
	if !isValidObjectName(object) {
		return errors.Trace(ObjectNameInvalid{bucket, object})
	}

	dstFile := pathJoin(s.TempDir, mustGetUUID())
	defer fsRemoveFile(dstFile)

	var siaObj = pathJoin(s.RootDir, bucket, object)
	if err := get(s.Address, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(dstFile), s.password); err != nil {
		return err
	}

	reader, size, err := fsOpenFile(dstFile, startOffset)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	defer reader.Close()

	// For negative length we read everything.
	if length < 0 {
		length = size - startOffset
	}

	bufSize := int64(readSizeV1)
	if bufSize > length {
		bufSize = length
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > size || startOffset+length > size {
		return errors.Trace(InvalidRange{startOffset, length, size})
	}

	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, length), buf)

	return err
}

// findSiaObject retrieves the siaObjectInfo for the Sia object with the given
// Sia path name.
func (s *siaObjects) findSiaObject(siaPath string) (siaObjectInfo, error) {
	sObjs, err := s.listRenterFiles("")
	if err != nil {
		return siaObjectInfo{}, err
	}

	for _, sObj := range sObjs {
		if sObj.SiaPath == siaPath {
			return sObj, nil
		}
	}

	return siaObjectInfo{}, errors.Trace(ObjectNotFound{"", siaPath})
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(bucket string, object string) (ObjectInfo, error) {
	siaPath := pathJoin(s.RootDir, bucket, object)
	so, err := s.findSiaObject(siaPath)
	if err != nil {
		return ObjectInfo{}, err
	}

	// Metadata about sia objects is just quite minimal. Sia only provides
	// file size.
	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
		Size:   int64(so.Filesize),
		IsDir:  false,
	}, nil
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	// Check the object's name first
	if !isValidObjectName(object) {
		return objInfo, errors.Trace(ObjectNameInvalid{bucket, object})
	}

	bufSize := int64(readSizeV1)
	size := data.Size()
	if size > 0 && bufSize > size {
		bufSize = size
	}
	buf := make([]byte, int(bufSize))

	srcFile := pathJoin(s.TempDir, mustGetUUID())

	if _, err = fsCreateFile(srcFile, data, buf, data.Size()); err != nil {
		return objInfo, err
	}

	var siaPath = pathJoin(s.RootDir, bucket, object)
	if err = post(s.Address, "/renter/upload/"+siaPath, "source="+srcFile, s.password); err != nil {
		fsRemoveFile(srcFile)
		return objInfo, err
	}
	defer s.deleteTempFileWhenUploadCompletes(srcFile, siaPath)

	objInfo = ObjectInfo{
		Name:    object,
		Bucket:  bucket,
		ModTime: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		Size:    size,
		ETag:    genETag(),
	}

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
	SiaPath        string  `json:"siapath"`
	LocalPath      string  `json:"localpath"`
	Filesize       uint64  `json:"filesize"`
	Available      bool    `json:"available"`
	Renewing       bool    `json:"renewing"`
	Redundancy     float64 `json:"redundancy"`
	UploadProgress float64 `json:"uploadprogress"`
}

// isValidObjectName returns whether or not the objectName provided is suitable for Sia
func isValidObjectName(objectName string) bool {
	reg, _ := regexp.Compile("[^a-zA-Z0-9., _/\\\\+-]+")
	return objectName == reg.ReplaceAllString(objectName, "")
}

type renterFiles struct {
	Files []siaObjectInfo `json:"files"`
}

// listRenterFiles will return a list of existing objects in the bucket provided
func (s *siaObjects) listRenterFiles(bucket string) (siaObjs []siaObjectInfo, err error) {
	// Get list of all renter files
	var rf renterFiles
	if err = list(s.Address, s.password, &rf); err != nil {
		return siaObjs, err
	}

	var prefix string
	root := s.RootDir + "/"
	if bucket == "" {
		prefix = root
	} else {
		prefix = root + bucket + "/"
	}

	for _, f := range rf.Files {
		if strings.HasPrefix(f.SiaPath, prefix) {
			siaObjs = append(siaObjs, f)
		}
	}

	return siaObjs, nil
}

// deleteTempFileWhenUploadCompletes checks the status of a Sia file upload
// until it reaches 100% upload progress, then deletes the local temp copy from
// the filesystem.
func (s *siaObjects) deleteTempFileWhenUploadCompletes(tempFile string, siaPath string) {
	var soi siaObjectInfo
	// Wait until 100% upload instead of 1x redundancy because if we delete
	// after 1x redundancy, the user has to pay the cost of other hosts
	// redistributing the file.
	for soi.UploadProgress < 100.0 {
		var err error
		soi, err = s.findSiaObject(siaPath)
		if err != nil {
			errorIf(err, "Unable to find file uploaded to Sia path %s", siaPath)
			break
		}

		// Sleep between each check so that we're not hammering the Sia
		// daemon with requests.
		time.Sleep(15 * time.Second)
	}
	fsRemoveFile(tempFile)
}
