package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type warmBackendAzure struct {
	serviceURL azblob.ServiceURL
}

func (az *warmBackendAzure) Put(bucket, object string, r io.Reader, length int64) error {
	blobURL := az.serviceURL.NewContainerURL(bucket).NewBlockBlobURL(object)

	_, err := azblob.UploadStreamToBlockBlob(context.Background(), r, blobURL, azblob.UploadStreamToBlockBlobOptions{})
	return err
}

func (az *warmBackendAzure) Get(bucket, object string) (r io.ReadCloser, err error) {
	blobURL := az.serviceURL.NewContainerURL(bucket).NewBlobURL(object)
	blob, err := blobURL.Download(context.Background(), 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}

	rc := blob.Body(azblob.RetryReaderOptions{})
	return rc, nil
}

func (az *warmBackendAzure) Remove(bucket, object string) error {
	blob := az.serviceURL.NewContainerURL(bucket).NewBlobURL(object)
	_, err := blob.Delete(context.Background(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

func newWarmBackendAzure(endpoint, accessKey, secretKey string) (*warmBackendAzure, error) {
	credential, err := azblob.NewSharedKeyCredential(accessKey, secretKey)
	if err != nil {
		if _, ok := err.(base64.CorruptInputError); ok {
			return nil, errors.New("invalid Azure credentials")
		}
		return &warmBackendAzure{}, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accessKey))
	serviceURL := azblob.NewServiceURL(*u, p)
	return &warmBackendAzure{serviceURL}, nil
}
