// +build ignore

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

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
)

func main() {
	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY and my-bucketname are
	// dummy values, please replace them with original values.

	// API requests are secure (HTTPS) if secure=true and insecure (HTTP) otherwise.
	// New returns an MinIO Admin client object.
	madmClnt, err := madmin.New("your-minio.example.com:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
	if err != nil {
		log.Fatalln(err)
	}
	ctx := context.Background()
	creds, err := auth.CreateCredentials("access-key", "secret-key")
	if err != nil {
		log.Fatalln(err)
	}
	target := madmin.BucketTarget{Endpoint: "site2:9000", Credentials: creds, TargetBucket: "destbucket", IsSSL: false, Type: madmin.ReplicationArn, BandwidthLimit: 2 * 1024 * 1024}
	// Set bucket target
	arn, err := madmClnt.SetBucketTarget(ctx, "srcbucket", &target)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("replication target ARN is:", arn)
	// List all bucket target(s)
	target, err = madmClnt.ListBucketTargets(ctx, "srcbucket", "")
	if err != nil {
		log.Fatalln(err)
	}
	// Get bucket target for arn type "replica"
	target, err = madmClnt.ListBucketTargets(ctx, "srcbucket", "replica")
	if err != nil {
		log.Fatalln(err)
	}
	// update credentials for target
	creds, err := auth.CreateCredentials("access-key2", "secret-key2")
	if err != nil {
		log.Fatalln(err)
	}
	target := madmin.BucketTarget{Endpoint: "site2:9000", Credentials: creds, SourceBucket: "srcbucket", TargetBucket: "destbucket", IsSSL: false, Arn: "arn:minio:ilm:us-east-1:3cbe15b8-82b9-44bc-a737-db9051ab359a:srcbucket"}
	// update credentials on bucket target
	if _, err := madmClnt.UpdateBucketTarget(ctx, &target); err != nil {
		log.Fatalln(err)
	}

	// Remove bucket target
	arn := "arn:minio:replica::ac66b2cf-dd8f-4e7e-a882-9a64132f0d59:dest"
	if err := madmClnt.RemoveBucketTarget(ctx, "srcbucket", arn); err != nil {
		log.Fatalln(err)
	}

}
