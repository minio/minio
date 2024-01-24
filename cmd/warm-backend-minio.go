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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type warmBackendMinIO struct {
	warmBackendS3
}

var _ WarmBackend = (*warmBackendMinIO)(nil)

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

	getRemoteTierTargetInstanceTransportOnce.Do(func() {
		getRemoteTierTargetInstanceTransport = NewHTTPTransportWithTimeout(10 * time.Minute)
	})
	opts := &minio.Options{
		Creds:     creds,
		Secure:    u.Scheme == "https",
		Transport: getRemoteTierTargetInstanceTransport,
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
