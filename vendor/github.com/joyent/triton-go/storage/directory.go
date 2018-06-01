//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/joyent/triton-go/client"
	"github.com/pkg/errors"
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

// ListDirectoryInput represents parameters to a List operation.
type ListDirectoryInput struct {
	DirectoryName string
	Limit         uint64
	Marker        string
}

// ListDirectoryOutput contains the outputs of a List operation.
type ListDirectoryOutput struct {
	Entries       []*DirectoryEntry
	ResultSetSize uint64
}

// List lists the contents of a directory on the Triton Object Store service.
func (s *DirectoryClient) List(ctx context.Context, input *ListDirectoryInput) (*ListDirectoryOutput, error) {
	absPath := absFileInput(s.client.AccountName, input.DirectoryName)
	query := &url.Values{}
	if input.Limit != 0 {
		query.Set("limit", strconv.FormatUint(input.Limit, 10))
	}
	if input.Marker != "" {
		query.Set("manta_path", input.Marker)
	}

	reqInput := client.RequestInput{
		Method: http.MethodGet,
		Path:   string(absPath),
		Query:  query,
	}
	respBody, respHeader, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list directory")
	}
	defer respBody.Close()

	var results []*DirectoryEntry
	scanner := bufio.NewScanner(respBody)
	for scanner.Scan() {
		current := &DirectoryEntry{}
		if err := json.Unmarshal(scanner.Bytes(), current); err != nil {
			return nil, errors.Wrap(err, "unable to decode list directories response")
		}

		results = append(results, current)
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "unable to decode list directories response")
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

// PutDirectoryInput represents parameters to a Put operation.
type PutDirectoryInput struct {
	DirectoryName string
}

// Put puts a director into the Triton Object Storage service is an idempotent
// create-or-update operation. Your private namespace starts at /:login, and you
// can create any nested set of directories or objects within it.
func (s *DirectoryClient) Put(ctx context.Context, input *PutDirectoryInput) error {
	absPath := absFileInput(s.client.AccountName, input.DirectoryName)

	headers := &http.Header{}
	headers.Set("Content-Type", "application/json; type=directory")

	reqInput := client.RequestInput{
		Method:  http.MethodPut,
		Path:    string(absPath),
		Headers: headers,
	}
	respBody, _, err := s.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errors.Wrap(err, "unable to put directory")
	}

	return nil
}

// DeleteDirectoryInput represents parameters to a Delete operation.
type DeleteDirectoryInput struct {
	DirectoryName string
	ForceDelete   bool //Will recursively delete all child directories and objects
}

// Delete deletes a directory on the Triton Object Storage. The directory must
// be empty.
func (s *DirectoryClient) Delete(ctx context.Context, input *DeleteDirectoryInput) error {
	absPath := absFileInput(s.client.AccountName, input.DirectoryName)

	if input.ForceDelete {
		err := deleteAll(*s, ctx, absPath)
		if err != nil {
			return err
		}
	} else {
		err := deleteDirectory(*s, ctx, absPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteAll(c DirectoryClient, ctx context.Context, directoryPath _AbsCleanPath) error {
	objs, err := c.List(ctx, &ListDirectoryInput{
		DirectoryName: string(directoryPath),
	})
	if err != nil {
		return err
	}
	for _, obj := range objs.Entries {
		newPath := absFileInput(c.client.AccountName, path.Join(string(directoryPath), obj.Name))
		if obj.Type == "directory" {
			err := deleteDirectory(c, ctx, newPath)
			if err != nil {
				return deleteAll(c, ctx, newPath)
			}
		} else {
			return deleteObject(c, ctx, newPath)
		}
	}

	return nil
}

func deleteDirectory(c DirectoryClient, ctx context.Context, directoryPath _AbsCleanPath) error {
	reqInput := client.RequestInput{
		Method: http.MethodDelete,
		Path:   string(directoryPath),
	}
	respBody, _, err := c.client.ExecuteRequestStorage(ctx, reqInput)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errors.Wrap(err, "unable to delete directory")
	}

	return nil
}

func deleteObject(c DirectoryClient, ctx context.Context, path _AbsCleanPath) error {
	objClient := &ObjectsClient{
		client: c.client,
	}

	err := objClient.Delete(ctx, &DeleteObjectInput{
		ObjectPath: string(path),
	})
	if err != nil {
		return err
	}

	return nil
}
