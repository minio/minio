package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/errwrap"
	"github.com/joyent/triton-go/client"
)

type ObjectsClient struct {
	client *client.Client
}

// GetObjectInput represents parameters to a GetObject operation.
type GetInfoInput struct {
	ObjectPath string
	Headers    map[string]string
}

// GetObjectOutput contains the outputs for a GetObject operation. It is your
// responsibility to ensure that the io.ReadCloser ObjectReader is closed.
type GetInfoOutput struct {
	ContentLength uint64
	ContentType   string
	LastModified  time.Time
	ContentMD5    string
	ETag          string
	Metadata      map[string]string
}

// GetInfo sends a HEAD request to an object in the Manta service. This function
// does not return a response body.
func (s *ObjectsClient) GetInfo(ctx context.Context, input *GetInfoInput) (*GetInfoOutput, error) {
	absPath := absFileInput(s.client.AccountName, input.ObjectPath)

	headers := &http.Header{}
	for key, value := range input.Headers {
		headers.Set(key, value)
	}

	reqInput := client.RequestInput{
		Method:  http.MethodHead,
		Path:    string(absPath),
		Headers: headers,
	}
	_, respHeaders, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if err != nil {
		return nil, errwrap.Wrapf("Error executing get info request: {{err}}", err)
	}

	response := &GetInfoOutput{
		ContentType: respHeaders.Get("Content-Type"),
		ContentMD5:  respHeaders.Get("Content-MD5"),
		ETag:        respHeaders.Get("Etag"),
	}

	lastModified, err := time.Parse(time.RFC1123, respHeaders.Get("Last-Modified"))
	if err == nil {
		response.LastModified = lastModified
	}

	contentLength, err := strconv.ParseUint(respHeaders.Get("Content-Length"), 10, 64)
	if err == nil {
		response.ContentLength = contentLength
	}

	metadata := map[string]string{}
	for key, values := range respHeaders {
		if strings.HasPrefix(key, "m-") {
			metadata[key] = strings.Join(values, ", ")
		}
	}
	response.Metadata = metadata

	return response, nil
}

// IsDir is a convenience wrapper around the GetInfo function which takes an
// ObjectPath and returns a boolean whether or not the object is a directory
// type in Manta. Returns an error if GetInfo failed upstream for some reason.
func (s *ObjectsClient) IsDir(ctx context.Context, objectPath string) (bool, error) {
	info, err := s.GetInfo(ctx, &GetInfoInput{
		ObjectPath: objectPath,
	})
	if err != nil {
		return false, err
	}
	if info != nil {
		return strings.HasSuffix(info.ContentType, "type=directory"), nil
	}
	return false, nil
}

// GetObjectInput represents parameters to a GetObject operation.
type GetObjectInput struct {
	ObjectPath string
	Headers    map[string]string
}

// GetObjectOutput contains the outputs for a GetObject operation. It is your
// responsibility to ensure that the io.ReadCloser ObjectReader is closed.
type GetObjectOutput struct {
	ContentLength uint64
	ContentType   string
	LastModified  time.Time
	ContentMD5    string
	ETag          string
	Metadata      map[string]string
	ObjectReader  io.ReadCloser
}

// Get retrieves an object from the Manta service. If error is nil (i.e. the
// call returns successfully), it is your responsibility to close the
// io.ReadCloser named ObjectReader in the operation output.
func (s *ObjectsClient) Get(ctx context.Context, input *GetObjectInput) (*GetObjectOutput, error) {
	absPath := absFileInput(s.client.AccountName, input.ObjectPath)

	headers := &http.Header{}
	for key, value := range input.Headers {
		headers.Set(key, value)
	}

	reqInput := client.RequestInput{
		Method:  http.MethodGet,
		Path:    string(absPath),
		Headers: headers,
	}
	respBody, respHeaders, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if err != nil {
		return nil, errwrap.Wrapf("Error executing Get request: {{err}}", err)
	}

	response := &GetObjectOutput{
		ContentType:  respHeaders.Get("Content-Type"),
		ContentMD5:   respHeaders.Get("Content-MD5"),
		ETag:         respHeaders.Get("Etag"),
		ObjectReader: respBody,
	}

	lastModified, err := time.Parse(time.RFC1123, respHeaders.Get("Last-Modified"))
	if err == nil {
		response.LastModified = lastModified
	}

	contentLength, err := strconv.ParseUint(respHeaders.Get("Content-Length"), 10, 64)
	if err == nil {
		response.ContentLength = contentLength
	}

	metadata := map[string]string{}
	for key, values := range respHeaders {
		if strings.HasPrefix(key, "m-") {
			metadata[key] = strings.Join(values, ", ")
		}
	}
	response.Metadata = metadata

	return response, nil
}

// DeleteObjectInput represents parameters to a DeleteObject operation.
type DeleteObjectInput struct {
	ObjectPath string
	Headers    map[string]string
}

// DeleteObject deletes an object.
func (s *ObjectsClient) Delete(ctx context.Context, input *DeleteObjectInput) error {
	absPath := absFileInput(s.client.AccountName, input.ObjectPath)

	headers := &http.Header{}
	for key, value := range input.Headers {
		headers.Set(key, value)
	}

	reqInput := client.RequestInput{
		Method:  http.MethodDelete,
		Path:    string(absPath),
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing Delete request: {{err}}", err)
	}

	return nil
}

// PutObjectMetadataInput represents parameters to a PutObjectMetadata operation.
type PutObjectMetadataInput struct {
	ObjectPath  string
	ContentType string
	Metadata    map[string]string
}

// PutObjectMetadata allows you to overwrite the HTTP headers for an already
// existing object, without changing the data. Note this is an idempotent "replace"
// operation, so you must specify the complete set of HTTP headers you want
// stored on each request.
//
// You cannot change "critical" headers:
// 	- Content-Length
//	- Content-MD5
//	- Durability-Level
func (s *ObjectsClient) PutMetadata(ctx context.Context, input *PutObjectMetadataInput) error {
	absPath := absFileInput(s.client.AccountName, input.ObjectPath)
	query := &url.Values{}
	query.Set("metadata", "true")

	headers := &http.Header{}
	headers.Set("Content-Type", input.ContentType)
	for key, value := range input.Metadata {
		headers.Set(key, value)
	}

	reqInput := client.RequestInput{
		Method:  http.MethodPut,
		Path:    string(absPath),
		Query:   query,
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing PutMetadata request: {{err}}", err)
	}

	return nil
}

// PutObjectInput represents parameters to a PutObject operation.
type PutObjectInput struct {
	ObjectPath       string
	DurabilityLevel  uint64
	ContentType      string
	ContentMD5       string
	IfMatch          string
	IfModifiedSince  *time.Time
	ContentLength    uint64
	MaxContentLength uint64
	ObjectReader     io.Reader
	Headers          map[string]string
	ForceInsert      bool //Force the creation of the directory tree
}

func (s *ObjectsClient) Put(ctx context.Context, input *PutObjectInput) error {
	absPath := absFileInput(s.client.AccountName, input.ObjectPath)

	if input.ForceInsert {
		absDirName := _AbsCleanPath(path.Dir(string(absPath)))
		exists, err := checkDirectoryTreeExists(*s, ctx, absDirName)
		if err != nil {
			return err
		}
		if !exists {
			err := createDirectory(*s, ctx, absDirName)
			if err != nil {
				return err
			}
			return putObject(*s, ctx, input, absPath)
		}
	}

	return putObject(*s, ctx, input, absPath)
}

// _AbsCleanPath is an internal type that means the input has been
// path.Clean()'ed and is an absolute path.
type _AbsCleanPath string

func absFileInput(accountName, objPath string) _AbsCleanPath {
	cleanInput := path.Clean(objPath)
	if strings.HasPrefix(cleanInput, path.Join("/", accountName, "/")) {
		return _AbsCleanPath(cleanInput)
	}

	cleanAbs := path.Clean(path.Join("/", accountName, objPath))
	return _AbsCleanPath(cleanAbs)
}

func putObject(c ObjectsClient, ctx context.Context, input *PutObjectInput, absPath _AbsCleanPath) error {
	if input.MaxContentLength != 0 && input.ContentLength != 0 {
		return errors.New("ContentLength and MaxContentLength may not both be set to non-zero values.")
	}

	headers := &http.Header{}
	for key, value := range input.Headers {
		headers.Set(key, value)
	}
	if input.DurabilityLevel != 0 {
		headers.Set("Durability-Level", strconv.FormatUint(input.DurabilityLevel, 10))
	}
	if input.ContentType != "" {
		headers.Set("Content-Type", input.ContentType)
	}
	if input.ContentMD5 != "" {
		headers.Set("Content-MD$", input.ContentMD5)
	}
	if input.IfMatch != "" {
		headers.Set("If-Match", input.IfMatch)
	}
	if input.IfModifiedSince != nil {
		headers.Set("If-Modified-Since", input.IfModifiedSince.Format(time.RFC1123))
	}
	if input.ContentLength != 0 {
		headers.Set("Content-Length", strconv.FormatUint(input.ContentLength, 10))
	}
	if input.MaxContentLength != 0 {
		headers.Set("Max-Content-Length", strconv.FormatUint(input.MaxContentLength, 10))
	}

	reqInput := client.RequestNoEncodeInput{
		Method:  http.MethodPut,
		Path:    string(absPath),
		Headers: headers,
		Body:    input.ObjectReader,
	}
	respBody, _, err := c.client.ExecuteRequestNoEncode(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing Put request: {{err}}", err)
	}

	return nil
}

func createDirectory(c ObjectsClient, ctx context.Context, absPath _AbsCleanPath) error {
	dirClient := &DirectoryClient{
		client: c.client,
	}

	// An abspath starts w/ a leading "/" which gets added to the slice as an
	// empty string. Start all array math at 1.
	parts := strings.Split(string(absPath), "/")
	if len(parts) < 2 {
		return errors.New("no path components to create directory")
	}

	folderPath := parts[1]
	// Don't attempt to create a manta account as a directory
	for i := 2; i < len(parts); i++ {
		part := parts[i]
		folderPath = path.Clean(path.Join("/", folderPath, part))
		err := dirClient.Put(ctx, &PutDirectoryInput{
			DirectoryName: folderPath,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func checkDirectoryTreeExists(c ObjectsClient, ctx context.Context, absPath _AbsCleanPath) (bool, error) {
	exists, err := c.IsDir(ctx, string(absPath))
	if err != nil {
		errType := &client.MantaError{}
		if errwrap.ContainsType(err, errType) {
			mantaErr := errwrap.GetType(err, errType).(*client.MantaError)
			if mantaErr.StatusCode == http.StatusNotFound {
				return false, nil
			}
		}
		return false, err
	}
	if exists {
		return true, nil
	}

	return false, nil
}
