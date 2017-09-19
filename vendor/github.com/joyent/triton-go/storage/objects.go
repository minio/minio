package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
type GetObjectInput struct {
	ObjectPath string
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

// GetObject retrieves an object from the Manta service. If error is nil (i.e.
// the call returns successfully), it is your responsibility to close the io.ReadCloser
// named ObjectReader in the operation output.
func (s *ObjectsClient) Get(ctx context.Context, input *GetObjectInput) (*GetObjectOutput, error) {
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.ObjectPath)

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
	}
	respBody, respHeaders, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if err != nil {
		return nil, errwrap.Wrapf("Error executing GetDirectory request: {{err}}", err)
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
}

// DeleteObject deletes an object.
func (s *ObjectsClient) Delete(ctx context.Context, input *DeleteObjectInput) error {
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.ObjectPath)

	reqInput := client.RequestInput{
		Method: http.MethodDelete,
		Path:   path,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing DeleteObject request: {{err}}", err)
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
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.ObjectPath)
	query := &url.Values{}
	query.Set("metadata", "true")

	headers := &http.Header{}
	headers.Set("Content-Type", input.ContentType)
	for key, value := range input.Metadata {
		headers.Set(key, value)
	}

	reqInput := client.RequestInput{
		Method:  http.MethodPut,
		Path:    path,
		Query:   query,
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing PutObjectMetadata request: {{err}}", err)
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
	ObjectReader     io.ReadSeeker
}

func (s *ObjectsClient) Put(ctx context.Context, input *PutObjectInput) error {
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.ObjectPath)

	if input.MaxContentLength != 0 && input.ContentLength != 0 {
		return errors.New("ContentLength and MaxContentLength may not both be set to non-zero values.")
	}

	headers := &http.Header{}
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
		Path:    path,
		Headers: headers,
		Body:    input.ObjectReader,
	}
	respBody, _, err := s.client.ExecuteRequestNoEncode(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing PutObjectMetadata request: {{err}}", err)
	}

	return nil
}
