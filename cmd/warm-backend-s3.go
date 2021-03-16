/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/madmin"
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

func (s3 *warmBackendS3) Put(ctx context.Context, object string, r io.Reader, length int64) error {
	_, err := s3.client.PutObject(ctx, s3.Bucket, s3.getDest(object), r, length, minio.PutObjectOptions{StorageClass: s3.StorageClass})
	return s3.ToObjectError(err, object)
}

func (s3 *warmBackendS3) Get(ctx context.Context, object string, opts warmBackendGetOpts) (io.ReadCloser, error) {
	gopts := minio.GetObjectOptions{}

	if opts.startOffset >= 0 && opts.length > 0 {
		if err := gopts.SetRange(opts.startOffset, opts.startOffset+opts.length-1); err != nil {
			return nil, s3.ToObjectError(err, object)
		}
	}
	c := &miniogo.Core{Client: s3.client}
	// Important to use core primitives here to pass range get options as is.
	r, _, _, err := c.GetObject(ctx, s3.Bucket, s3.getDest(object), gopts)
	if err != nil {
		return nil, s3.ToObjectError(err, object)
	}
	return r, nil
}

func (s3 *warmBackendS3) Remove(ctx context.Context, object string) error {
	err := s3.client.RemoveObject(ctx, s3.Bucket, s3.getDest(object), minio.RemoveObjectOptions{})
	return s3.ToObjectError(err, object)
}

func (s3 *warmBackendS3) InUse(ctx context.Context) (bool, error) {
	result, err := s3.core.ListObjectsV2(s3.Bucket, s3.Prefix, "", false, "/", 1)
	if err != nil {
		return false, s3.ToObjectError(err)
	}
	if len(result.CommonPrefixes) > 0 || len(result.Contents) > 0 {
		return true, nil
	}
	return false, nil
}

func newWarmBackendS3(conf madmin.TierS3) (*warmBackendS3, error) {
	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return nil, err
	}
	creds := credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, "")
	getRemoteTargetInstanceTransportOnce.Do(func() {
		getRemoteTargetInstanceTransport = newGatewayHTTPTransport(10 * time.Minute)
	})
	opts := &minio.Options{
		Creds:     creds,
		Secure:    u.Scheme == "https",
		Transport: getRemoteTargetInstanceTransport,
	}
	client, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, err
	}
	core, err := minio.NewCore(u.Host, opts)
	if err != nil {
		return nil, err
	}
	return &warmBackendS3{
		client:       client,
		core:         core,
		Bucket:       conf.Bucket,
		Prefix:       strings.TrimSuffix(conf.Prefix, slashSeparator),
		StorageClass: conf.StorageClass,
	}, nil
}
