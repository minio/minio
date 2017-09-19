package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/hashicorp/errwrap"
	"github.com/joyent/triton-go/client"
)

type DirectoryClient struct {
	client *client.Client
}

// DirectoryEntry represents an object or directory in Manta.
type DirectoryEntry struct {
	ETag         string    `json:"etag"`
	ModifiedTime time.Time `json:"mtime"`
	Name         string    `json:"name"`
	Size         uint64    `json:"size"`
	Type         string    `json:"type"`
}

// ListDirectoryInput represents parameters to a ListDirectory operation.
type ListDirectoryInput struct {
	DirectoryName string
	Limit         uint64
	Marker        string
}

// ListDirectoryOutput contains the outputs of a ListDirectory operation.
type ListDirectoryOutput struct {
	Entries       []*DirectoryEntry
	ResultSetSize uint64
}

// List lists the contents of a directory on the Triton Object Store service.
func (s *DirectoryClient) List(ctx context.Context, input *ListDirectoryInput) (*ListDirectoryOutput, error) {
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.DirectoryName)
	query := &url.Values{}
	if input.Limit != 0 {
		query.Set("limit", strconv.FormatUint(input.Limit, 10))
	}
	if input.Marker != "" {
		query.Set("manta_path", input.Marker)
	}

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   path,
		Query:  query,
	}
	respBody, respHeader, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing ListDirectory request: {{err}}", err)
	}

	var results []*DirectoryEntry
	for {
		current := &DirectoryEntry{}
		decoder := json.NewDecoder(respBody)
		if err = decoder.Decode(&current); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errwrap.Wrapf("Error decoding ListDirectory response: {{err}}", err)
		}
		results = append(results, current)
	}

	output := &ListDirectoryOutput{
		Entries: results,
	}

	resultSetSize, err := strconv.ParseUint(respHeader.Get("Result-Set-Size"), 10, 64)
	if err == nil {
		output.ResultSetSize = resultSetSize
	}

	return output, nil
}

// PutDirectoryInput represents parameters to a PutDirectory operation.
type PutDirectoryInput struct {
	DirectoryName string
}

// Put puts a directoy into the Triton Object Storage service is an idempotent
// create-or-update operation. Your private namespace starts at /:login, and you
// can create any nested set of directories or objects within it.
func (s *DirectoryClient) Put(ctx context.Context, input *PutDirectoryInput) error {
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.DirectoryName)
	headers := &http.Header{}
	headers.Set("Content-Type", "application/json; type=directory")

	reqInput := client.RequestInput{
		Method:  http.MethodPut,
		Path:    path,
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing PutDirectory request: {{err}}", err)
	}

	return nil
}

// DeleteDirectoryInput represents parameters to a DeleteDirectory operation.
type DeleteDirectoryInput struct {
	DirectoryName string
}

// Delete deletes a directory on the Triton Object Storage. The directory must
// be empty.
func (s *DirectoryClient) Delete(ctx context.Context, input *DeleteDirectoryInput) error {
	path := fmt.Sprintf("/%s%s", s.client.AccountName, input.DirectoryName)

	reqInput := client.RequestInput{
		Method: http.MethodDelete,
		Path:   path,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing DeleteDirectory request: {{err}}", err)
	}

	return nil
}
