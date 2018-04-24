//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package storage

import (
	"net/http"

	triton "github.com/joyent/triton-go"
	"github.com/joyent/triton-go/client"
)

type StorageClient struct {
	Client *client.Client
}

func newStorageClient(client *client.Client) *StorageClient {
	return &StorageClient{
		Client: client,
	}
}

// NewClient returns a new client for working with Storage endpoints and
// resources within CloudAPI
func NewClient(config *triton.ClientConfig) (*StorageClient, error) {
	// TODO: Utilize config interface within the function itself
	client, err := client.New(config.TritonURL, config.MantaURL, config.AccountName, config.Signers...)
	if err != nil {
		return nil, err
	}
	return newStorageClient(client), nil
}

// SetHeader allows a consumer of the current client to set a custom header for
// the next backend HTTP request sent to CloudAPI.
func (c *StorageClient) SetHeader(header *http.Header) {
	c.Client.RequestHeader = header
}

// Dir returns a DirectoryClient used for accessing functions pertaining to
// Directories functionality of the Manta API.
func (c *StorageClient) Dir() *DirectoryClient {
	return &DirectoryClient{c.Client}
}

// Jobs returns a JobClient used for accessing functions pertaining to Jobs
// functionality of the Triton Object Storage API.
func (c *StorageClient) Jobs() *JobClient {
	return &JobClient{c.Client}
}

// Objects returns an ObjectsClient used for accessing functions pertaining to
// Objects functionality of the Triton Object Storage API.
func (c *StorageClient) Objects() *ObjectsClient {
	return &ObjectsClient{c.Client}
}

// SnapLinks returns an SnapLinksClient used for accessing functions pertaining to
// SnapLinks functionality of the Triton Object Storage API.
func (c *StorageClient) SnapLinks() *SnapLinksClient {
	return &SnapLinksClient{c.Client}
}
