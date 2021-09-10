// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/minio/madmin-go"
)

type warmBackendAzure struct {
	serviceURL   azblob.ServiceURL
	Bucket       string
	Prefix       string
	StorageClass string
}

func (az *warmBackendAzure) getDest(object string) string {
	destObj := object
	if az.Prefix != "" {
		destObj = fmt.Sprintf("%s/%s", az.Prefix, object)
	}
	return destObj
}
func (az *warmBackendAzure) tier() azblob.AccessTierType {
	for _, t := range azblob.PossibleAccessTierTypeValues() {
		if strings.ToLower(az.StorageClass) == strings.ToLower(string(t)) {
			return t
		}
	}
	return azblob.AccessTierType("")
}

// FIXME: add support for remote version ID in Azure remote tier and remove
// this. Currently it's a no-op.

func (az *warmBackendAzure) Put(ctx context.Context, object string, r io.Reader, length int64) (remoteVersionID, error) {
	blobURL := az.serviceURL.NewContainerURL(az.Bucket).NewBlockBlobURL(az.getDest(object))
	// set tier if specified -
	if az.StorageClass != "" {
		if _, err := blobURL.SetTier(ctx, az.tier(), azblob.LeaseAccessConditions{}); err != nil {
			return "", azureToObjectError(err, az.Bucket, object)
		}
	}
	res, err := azblob.UploadStreamToBlockBlob(ctx, r, blobURL, azblob.UploadStreamToBlockBlobOptions{})
	return remoteVersionID(res.Version()), azureToObjectError(err, az.Bucket, object)
}

func (az *warmBackendAzure) Get(ctx context.Context, object string, rv remoteVersionID, opts WarmBackendGetOpts) (r io.ReadCloser, err error) {
	if opts.startOffset < 0 {
		return nil, InvalidRange{}
	}
	blobURL := az.serviceURL.NewContainerURL(az.Bucket).NewBlobURL(az.getDest(object))
	blob, err := blobURL.Download(ctx, opts.startOffset, opts.length, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, azureToObjectError(err, az.Bucket, object)
	}

	rc := blob.Body(azblob.RetryReaderOptions{})
	return rc, nil
}

func (az *warmBackendAzure) Remove(ctx context.Context, object string, rv remoteVersionID) error {
	blob := az.serviceURL.NewContainerURL(az.Bucket).NewBlobURL(az.getDest(object))
	_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return azureToObjectError(err, az.Bucket, object)
}

func (az *warmBackendAzure) InUse(ctx context.Context) (bool, error) {
	containerURL := az.serviceURL.NewContainerURL(az.Bucket)
	resp, err := containerURL.ListBlobsHierarchySegment(ctx, azblob.Marker{}, "/", azblob.ListBlobsSegmentOptions{
		Prefix:     az.Prefix,
		MaxResults: int32(1),
	})
	if err != nil {
		return false, azureToObjectError(err, az.Bucket, az.Prefix)
	}
	if len(resp.Segment.BlobPrefixes) > 0 || len(resp.Segment.BlobItems) > 0 {
		return true, nil
	}
	return false, nil
}

func newWarmBackendAzure(conf madmin.TierAzure) (*warmBackendAzure, error) {
	credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	if err != nil {
		if _, ok := err.(base64.CorruptInputError); ok {
			return nil, errors.New("invalid Azure credentials")
		}
		return nil, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", conf.AccountName))
	if err != nil {
		return nil, err
	}
	serviceURL := azblob.NewServiceURL(*u, p)
	return &warmBackendAzure{
		serviceURL:   serviceURL,
		Bucket:       conf.Bucket,
		Prefix:       strings.TrimSuffix(conf.Prefix, slashSeparator),
		StorageClass: conf.StorageClass,
	}, nil
}

// Convert azure errors to minio object layer errors.
func azureToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	azureErr, ok := err.(azblob.StorageError)
	if !ok {
		// We don't interpret non Azure errors. As azure errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	serviceCode := string(azureErr.ServiceCode())
	statusCode := azureErr.Response().StatusCode

	return azureCodesToObjectError(err, serviceCode, statusCode, bucket, object)
}

func azureCodesToObjectError(err error, serviceCode string, statusCode int, bucket string, object string) error {
	switch serviceCode {
	case "ContainerNotFound", "ContainerBeingDeleted":
		err = BucketNotFound{Bucket: bucket}
	case "ContainerAlreadyExists":
		err = BucketExists{Bucket: bucket}
	case "InvalidResourceName":
		err = BucketNameInvalid{Bucket: bucket}
	case "RequestBodyTooLarge":
		err = PartTooBig{}
	case "InvalidMetadata":
		err = UnsupportedMetadata{}
	case "BlobAccessTierNotSupportedForAccountType":
		err = NotImplemented{}
	case "OutOfRangeInput":
		err = ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	default:
		switch statusCode {
		case http.StatusNotFound:
			if object != "" {
				err = ObjectNotFound{
					Bucket: bucket,
					Object: object,
				}
			} else {
				err = BucketNotFound{Bucket: bucket}
			}
		case http.StatusBadRequest:
			err = BucketNameInvalid{Bucket: bucket}
		}
	}
	return err
}
