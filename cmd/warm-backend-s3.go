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
	"net/url"
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type warmBackendS3 struct {
	client       *minio.Client
	core         *minio.Core
	Bucket       string
	Prefix       string
	StorageClass string
}

func (s3 *warmBackendS3) ToObjectError(err error, params ...string) error {
	object := ""
	if len(params) >= 1 {
		object = params[0]
	}

	return ErrorRespToObjectError(err, s3.Bucket, s3.getDest(object))
}

func (s3 *warmBackendS3) getDest(object string) string {
	destObj := object
	if s3.Prefix != "" {
		destObj = fmt.Sprintf("%s/%s", s3.Prefix, object)
	}
	return destObj
}

func (s3 *warmBackendS3) PutWithMeta(ctx context.Context, object string, r io.Reader, length int64, meta map[string]string) (remoteVersionID, error) {
	res, err := s3.client.PutObject(ctx, s3.Bucket, s3.getDest(object), r, length, minio.PutObjectOptions{
		SendContentMd5: true,
		StorageClass:   s3.StorageClass,
		UserMetadata:   meta,
	})
	return remoteVersionID(res.VersionID), s3.ToObjectError(err, object)
}

func (s3 *warmBackendS3) Put(ctx context.Context, object string, r io.Reader, length int64) (remoteVersionID, error) {
	return s3.PutWithMeta(ctx, object, r, length, map[string]string{})
}

func (s3 *warmBackendS3) Get(ctx context.Context, object string, rv remoteVersionID, opts WarmBackendGetOpts) (io.ReadCloser, error) {
	gopts := minio.GetObjectOptions{}

	if rv != "" {
		gopts.VersionID = string(rv)
	}
	if opts.startOffset >= 0 && opts.length > 0 {
		if err := gopts.SetRange(opts.startOffset, opts.startOffset+opts.length-1); err != nil {
			return nil, s3.ToObjectError(err, object)
		}
	}
	c := &minio.Core{Client: s3.client}
	// Important to use core primitives here to pass range get options as is.
	r, _, _, err := c.GetObject(ctx, s3.Bucket, s3.getDest(object), gopts)
	if err != nil {
		return nil, s3.ToObjectError(err, object)
	}
	return r, nil
}

func (s3 *warmBackendS3) Remove(ctx context.Context, object string, rv remoteVersionID) error {
	ropts := minio.RemoveObjectOptions{}
	if rv != "" {
		ropts.VersionID = string(rv)
	}
	err := s3.client.RemoveObject(ctx, s3.Bucket, s3.getDest(object), ropts)
	return s3.ToObjectError(err, object)
}

func (s3 *warmBackendS3) InUse(ctx context.Context) (bool, error) {
	result, err := s3.core.ListObjectsV2(s3.Bucket, s3.Prefix, "", "", slashSeparator, 1)
	if err != nil {
		return false, s3.ToObjectError(err)
	}
	return len(result.CommonPrefixes) > 0 || len(result.Contents) > 0, nil
}

func newWarmBackendS3(conf madmin.TierS3, tier string) (*warmBackendS3, error) {
	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return nil, err
	}

	// Validation code
	switch {
	case conf.AWSRoleWebIdentityTokenFile == "" && conf.AWSRoleARN != "" || conf.AWSRoleWebIdentityTokenFile != "" && conf.AWSRoleARN == "":
		return nil, errors.New("both the token file and the role ARN are required")
	case conf.AccessKey == "" && conf.SecretKey != "" || conf.AccessKey != "" && conf.SecretKey == "":
		return nil, errors.New("both the access and secret keys are required")
	case conf.AWSRole && (conf.AWSRoleWebIdentityTokenFile != "" || conf.AWSRoleARN != "" || conf.AccessKey != "" || conf.SecretKey != ""):
		return nil, errors.New("AWS Role cannot be activated with static credentials or the web identity token file")
	case conf.Bucket == "":
		return nil, errors.New("no bucket name was provided")
	}

	// Credentials initialization
	var creds *credentials.Credentials
	switch {
	case conf.AWSRole:
		creds = credentials.New(&credentials.IAM{
			Client: &http.Client{
				Transport: NewHTTPTransport(),
			},
		})
	case conf.AWSRoleWebIdentityTokenFile != "" && conf.AWSRoleARN != "":
		sessionName := conf.AWSRoleSessionName
		if sessionName == "" {
			// RoleSessionName has a limited set of characters (https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
			sessionName = "minio-tier-" + mustGetUUID()
		}
		s3WebIdentityIAM := credentials.IAM{
			Client: &http.Client{
				Transport: NewHTTPTransport(),
			},
			EKSIdentity: struct {
				TokenFile       string
				RoleARN         string
				RoleSessionName string
			}{
				conf.AWSRoleWebIdentityTokenFile,
				conf.AWSRoleARN,
				sessionName,
			},
		}
		creds = credentials.New(&s3WebIdentityIAM)
	case conf.AccessKey != "" && conf.SecretKey != "":
		creds = credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, "")
	default:
		return nil, errors.New("insufficient parameters for S3 backend authentication")
	}
	opts := &minio.Options{
		Creds:     creds,
		Secure:    u.Scheme == "https",
		Transport: globalRemoteTargetTransport,
		Region:    conf.Region,
	}
	client, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, err
	}
	client.SetAppInfo(fmt.Sprintf("s3-tier-%s", tier), ReleaseTag)

	core := &minio.Core{Client: client}
	return &warmBackendS3{
		client:       client,
		core:         core,
		Bucket:       conf.Bucket,
		Prefix:       strings.TrimSuffix(conf.Prefix, slashSeparator),
		StorageClass: conf.StorageClass,
	}, nil
}
