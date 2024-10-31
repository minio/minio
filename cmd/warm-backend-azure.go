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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/minio/madmin-go/v3"
)

type warmBackendAzure struct {
	clnt         *azblob.Client
	Bucket       string
	Prefix       string
	StorageClass string
}

func (az *warmBackendAzure) tier() *blob.AccessTier {
	if az.StorageClass == "" {
		return nil
	}
	for _, t := range blob.PossibleAccessTierValues() {
		if strings.EqualFold(az.StorageClass, string(t)) {
			return &t
		}
	}
	return nil
}

func (az *warmBackendAzure) getDest(object string) string {
	destObj := object
	if az.Prefix != "" {
		destObj = fmt.Sprintf("%s/%s", az.Prefix, object)
	}
	return destObj
}

func (az *warmBackendAzure) PutWithMeta(ctx context.Context, object string, r io.Reader, length int64, meta map[string]string) (remoteVersionID, error) {
	azMeta := map[string]*string{}
	for k, v := range meta {
		azMeta[k] = to.Ptr(v)
	}
	resp, err := az.clnt.UploadStream(ctx, az.Bucket, az.getDest(object), io.LimitReader(r, length), &azblob.UploadStreamOptions{
		Concurrency: 4,
		AccessTier:  az.tier(), // set tier if specified
		Metadata:    azMeta,
	})
	if err != nil {
		return "", azureToObjectError(err, az.Bucket, az.getDest(object))
	}
	vid := ""
	if resp.VersionID != nil {
		vid = *resp.VersionID
	}
	return remoteVersionID(vid), nil
}

func (az *warmBackendAzure) Put(ctx context.Context, object string, r io.Reader, length int64) (remoteVersionID, error) {
	return az.PutWithMeta(ctx, object, r, length, map[string]string{})
}

func (az *warmBackendAzure) Get(ctx context.Context, object string, rv remoteVersionID, opts WarmBackendGetOpts) (r io.ReadCloser, err error) {
	if opts.startOffset < 0 {
		return nil, InvalidRange{}
	}
	resp, err := az.clnt.DownloadStream(ctx, az.Bucket, az.getDest(object), &azblob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: opts.startOffset, Count: opts.length},
	})
	if err != nil {
		return nil, azureToObjectError(err, az.Bucket, az.getDest(object))
	}

	return resp.Body, nil
}

func (az *warmBackendAzure) Remove(ctx context.Context, object string, rv remoteVersionID) error {
	_, err := az.clnt.DeleteBlob(ctx, az.Bucket, az.getDest(object), &azblob.DeleteBlobOptions{})
	return azureToObjectError(err, az.Bucket, az.getDest(object))
}

func (az *warmBackendAzure) InUse(ctx context.Context) (bool, error) {
	maxResults := int32(1)
	pager := az.clnt.NewListBlobsFlatPager(az.Bucket, &azblob.ListBlobsFlatOptions{
		Prefix:     &az.Prefix,
		MaxResults: &maxResults,
	})
	if !pager.More() {
		return false, nil
	}

	resp, err := pager.NextPage(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "no more pages") {
			return false, nil
		}
		return false, azureToObjectError(err, az.Bucket, az.Prefix)
	}

	return len(resp.Segment.BlobItems) > 0, nil
}

type azureConf struct {
	madmin.TierAzure
}

func (conf azureConf) Validate() error {
	switch {
	case conf.AccountName == "":
		return errors.New("the account name is required")
	case conf.AccountKey != "" && (conf.SPAuth.TenantID != "" || conf.SPAuth.ClientID != "" || conf.SPAuth.ClientSecret != ""):
		return errors.New("multiple authentication mechanisms are provided")
	case conf.AccountKey == "" && (conf.SPAuth.TenantID == "" || conf.SPAuth.ClientID == "" || conf.SPAuth.ClientSecret == ""):
		return errors.New("no authentication mechanism was provided")
	}

	if conf.Bucket == "" {
		return errors.New("no bucket name was provided")
	}

	return nil
}

func (conf azureConf) NewClient() (clnt *azblob.Client, clntErr error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	ep := conf.Endpoint
	if ep == "" {
		ep = fmt.Sprintf("https://%s.blob.core.windows.net", conf.AccountName)
	}

	if conf.IsSPEnabled() {
		credential, err := azidentity.NewClientSecretCredential(conf.SPAuth.TenantID, conf.SPAuth.ClientID, conf.SPAuth.ClientSecret, &azidentity.ClientSecretCredentialOptions{})
		if err != nil {
			return nil, err
		}
		return azblob.NewClient(ep, credential, &azblob.ClientOptions{})
	}
	credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	if err != nil {
		return nil, err
	}
	return azblob.NewClientWithSharedKeyCredential(ep, credential, &azblob.ClientOptions{})
}

func newWarmBackendAzure(conf madmin.TierAzure, _ string) (*warmBackendAzure, error) {
	clnt, err := azureConf{conf}.NewClient()
	if err != nil {
		return nil, err
	}

	return &warmBackendAzure{
		clnt:         clnt,
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

	azureErr, ok := err.(*azcore.ResponseError)
	if !ok {
		// We don't interpret non Azure errors. As azure errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	serviceCode := azureErr.ErrorCode
	statusCode := azureErr.StatusCode

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
