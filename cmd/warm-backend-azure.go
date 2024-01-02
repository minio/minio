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
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/minio/madmin-go/v3"
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
		if strings.EqualFold(az.StorageClass, string(t)) {
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
		if _, err := blobURL.SetTier(ctx, az.tier(), azblob.LeaseAccessConditions{}, azblob.RehydratePriorityStandard); err != nil {
			return "", azureToObjectError(err, az.Bucket, object)
		}
	}
	res, err := azblob.UploadStreamToBlockBlob(ctx, r, blobURL, azblob.UploadStreamToBlockBlobOptions{})
	if err != nil {
		return "", azureToObjectError(err, az.Bucket, object)
	}
	return remoteVersionID(res.Version()), nil
}

func (az *warmBackendAzure) Get(ctx context.Context, object string, rv remoteVersionID, opts WarmBackendGetOpts) (r io.ReadCloser, err error) {
	if opts.startOffset < 0 {
		return nil, InvalidRange{}
	}
	blobURL := az.serviceURL.NewContainerURL(az.Bucket).NewBlobURL(az.getDest(object))
	blob, err := blobURL.Download(ctx, opts.startOffset, opts.length, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
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

func newCredentialFromSP(conf madmin.TierAzure) (azblob.Credential, error) {
	oauthConfig, err := adal.NewOAuthConfig(azure.PublicCloud.ActiveDirectoryEndpoint, conf.SPAuth.TenantID)
	if err != nil {
		return nil, err
	}
	spt, err := adal.NewServicePrincipalToken(*oauthConfig, conf.SPAuth.ClientID, conf.SPAuth.ClientSecret, azure.PublicCloud.ResourceIdentifiers.Storage)
	if err != nil {
		return nil, err
	}

	// Refresh obtains a fresh token
	err = spt.Refresh()
	if err != nil {
		return nil, err
	}

	tc := azblob.NewTokenCredential(spt.Token().AccessToken, func(tc azblob.TokenCredential) time.Duration {
		err := spt.Refresh()
		if err != nil {
			return 0
		}
		// set the new token value
		tc.SetToken(spt.Token().AccessToken)

		// get the next token before the current one expires
		nextRenewal := float64(time.Until(spt.Token().Expires())) * 0.8
		if nextRenewal <= 0 {
			nextRenewal = float64(time.Second)
		}

		return time.Duration(nextRenewal)
	})

	return tc, nil
}

func newWarmBackendAzure(conf madmin.TierAzure, _ string) (*warmBackendAzure, error) {
	var (
		credential azblob.Credential
		err        error
	)

	switch {
	case conf.AccountName == "":
		return nil, errors.New("the account name is required")
	case conf.AccountKey != "" && (conf.SPAuth.TenantID != "" || conf.SPAuth.ClientID != "" || conf.SPAuth.ClientSecret != ""):
		return nil, errors.New("multiple authentication mechanisms are provided")
	case conf.AccountKey == "" && (conf.SPAuth.TenantID == "" || conf.SPAuth.ClientID == "" || conf.SPAuth.ClientSecret == ""):
		return nil, errors.New("no authentication mechanism was provided")
	}

	if conf.Bucket == "" {
		return nil, errors.New("no bucket name was provided")
	}

	if conf.IsSPEnabled() {
		credential, err = newCredentialFromSP(conf)
	} else {
		credential, err = azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	}
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
