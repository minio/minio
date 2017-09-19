package storage

import (
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
