// Copyright (c) 2015-2022 MinIO, Inc.
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
	"math"
	"net/url"
	"strings"

	"github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type warmBackendMinIO struct {
	warmBackendS3
}

var _ WarmBackend = (*warmBackendMinIO)(nil)

const (
	maxMultipartPutObjectSize = 1024 * 1024 * 1024 * 1024 * 5
	maxPartsCount             = 10000
	maxPartSize               = 1024 * 1024 * 1024 * 5
	minPartSize               = 1024 * 1024 * 128 // chosen by us to be optimal for HDDs
)

// optimalPartInfo - calculate the optimal part info for a given
// object size.
//
// NOTE: Assumption here is that for any object to be uploaded to any S3 compatible
// object storage it will have the following parameters as constants.
//
//	maxPartsCount - 10000
//	maxMultipartPutObjectSize - 5TiB
func optimalPartSize(objectSize int64) (partSize int64, err error) {
	// object size is '-1' set it to 5TiB.
	if objectSize == -1 {
		objectSize = maxMultipartPutObjectSize
	}

	// object size is larger than supported maximum.
	if objectSize > maxMultipartPutObjectSize {
		err = errors.New("entity too large")
		return partSize, err
	}

	configuredPartSize := minPartSize
	// Use floats for part size for all calculations to avoid
	// overflows during float64 to int64 conversions.
	partSizeFlt := float64(objectSize / maxPartsCount)
	partSizeFlt = math.Ceil(partSizeFlt/float64(configuredPartSize)) * float64(configuredPartSize)

	// Part size.
	partSize = int64(partSizeFlt)
	if partSize == 0 {
		return minPartSize, nil
	}
	return partSize, nil
}

func (m *warmBackendMinIO) PutWithMeta(ctx context.Context, object string, r io.Reader, length int64, meta map[string]string) (remoteVersionID, error) {
	partSize, err := optimalPartSize(length)
	if err != nil {
		return remoteVersionID(""), err
	}
	res, err := m.client.PutObject(ctx, m.Bucket, m.getDest(object), r, length, minio.PutObjectOptions{
		StorageClass:         m.StorageClass,
		PartSize:             uint64(partSize),
		DisableContentSha256: true,
		UserMetadata:         meta,
	})
	return remoteVersionID(res.VersionID), m.ToObjectError(err, object)
}

func (m *warmBackendMinIO) Put(ctx context.Context, object string, r io.Reader, length int64) (remoteVersionID, error) {
	return m.PutWithMeta(ctx, object, r, length, map[string]string{})
}

func newWarmBackendMinIO(conf madmin.TierMinIO, tier string) (*warmBackendMinIO, error) {
	// Validation of credentials
	if conf.AccessKey == "" || conf.SecretKey == "" {
		return nil, errors.New("both access and secret keys are required")
	}

	if conf.Bucket == "" {
		return nil, errors.New("no bucket name was provided")
	}

	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return nil, err
	}

	creds := credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, "")
	opts := &minio.Options{
		Creds:           creds,
		Secure:          u.Scheme == "https",
		Transport:       globalRemoteTargetTransport,
		TrailingHeaders: true,
	}
	client, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, err
	}
	client.SetAppInfo(fmt.Sprintf("minio-tier-%s", tier), ReleaseTag)

	core := &minio.Core{Client: client}
	return &warmBackendMinIO{
		warmBackendS3{
			client: client,
			core:   core,
			Bucket: conf.Bucket,
			Prefix: strings.TrimSuffix(conf.Prefix, slashSeparator),
		},
	}, nil
}
