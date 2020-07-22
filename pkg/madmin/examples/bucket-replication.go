// +build ignore

/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package main

import (
	"context"
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
	target := madmin.BucketReplicationTarget{Endpoint: "site2:9000", Credentials: creds, TargetBucket: "destbucket", IsSSL: false}
	// Set bucket replication target
	if err := madmClnt.SetBucketReplicationTarget(ctx, "srcbucket", &target); err != nil {
		log.Fatalln(err)
	}
	// Get bucket replication target
	target, err = madmClnt.GetBucketReplicationTarget(ctx, "srcbucket")
	if err != nil {
		log.Fatalln(err)
	}

	// Remove bucket replication target
	if err := madmClnt.SetBucketReplicationTarget(ctx, "srcbucket", nil); err != nil {
		log.Fatalln(err)
	}

}
