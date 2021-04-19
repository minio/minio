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

package madmin

import (
	"encoding/base64"
	"fmt"
	"log"

	"reflect"
	"testing"
)

func ExampleNewTierS3() {
	simpleS3SC, err := NewTierS3("simple-s3", "accessKey", "secretKey", "testbucket")
	if err != nil {
		log.Fatalln(err, "Failed to create s3 backed tier")
	}
	fmt.Println(simpleS3SC)

	fullyCustomS3SC, err := NewTierS3("custom-s3", "accessKey", "secretKey", "testbucket",
		S3Endpoint("https://s3.amazonaws.com"), S3Prefix("testprefix"), S3Region("us-west-1"), S3StorageClass("S3_IA"))
	if err != nil {
		log.Fatalln(err, "Failed to create s3 tier")
	}
	fmt.Println(fullyCustomS3SC)
}

func ExampleNewTierAzure() {
	simpleAzSC, err := NewTierAzure("simple-az", "accessKey", "secretKey", "testbucket")
	if err != nil {
		log.Fatalln(err, "Failed to create azure backed tier")
	}
	fmt.Println(simpleAzSC)

	fullyCustomAzSC, err := NewTierAzure("custom-az", "accessKey", "secretKey", "testbucket", AzureEndpoint("http://blob.core.windows.net"), AzurePrefix("testprefix"))
	if err != nil {
		log.Fatalln(err, "Failed to create azure backed tier")
	}
	fmt.Println(fullyCustomAzSC)
}

func ExampleNewTierGCS() {
	credsJSON := []byte("credentials json content goes here")
	simpleGCSSC, err := NewTierGCS("simple-gcs", credsJSON, "testbucket")
	if err != nil {
		log.Fatalln(err, "Failed to create GCS backed tier")
	}
	fmt.Println(simpleGCSSC)

	fullyCustomGCSSC, err := NewTierGCS("custom-gcs", credsJSON, "testbucket", GCSPrefix("testprefix"))
	if err != nil {
		log.Fatalln(err, "Failed to create GCS backed tier")
	}
	fmt.Println(fullyCustomGCSSC)
}

// TestS3Tier tests S3Options helpers
func TestS3Tier(t *testing.T) {
	scName := "test-s3"
	endpoint := "https://mys3.com"
	accessKey, secretKey := "accessKey", "secretKey"
	bucket, prefix := "testbucket", "testprefix"
	region := "us-west-1"
	storageClass := "S3_IA"

	want := &TierConfig{
		Version: TierConfigV1,
		Type:    S3,
		Name:    scName,
		S3: &TierS3{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Bucket:    bucket,

			// custom values
			Endpoint:     endpoint,
			Prefix:       prefix,
			Region:       region,
			StorageClass: storageClass,
		},
	}
	options := []S3Options{
		S3Endpoint(endpoint),
		S3Prefix(prefix),
		S3Region(region),
		S3StorageClass(storageClass),
	}
	got, err := NewTierS3(scName, accessKey, secretKey, bucket, options...)
	if err != nil {
		t.Fatalf("Failed to create a custom s3 tier %s", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got != want, got = %v want = %v", *got, *want)
	}
}

// TestAzTier tests AzureOptions helpers
func TestAzTier(t *testing.T) {
	scName := "test-az"
	endpoint := "https://myazure.com"

	accountName, accountKey := "accountName", "accountKey"
	bucket, prefix := "testbucket", "testprefix"
	region := "us-east-1"
	want := &TierConfig{
		Version: TierConfigV1,
		Type:    Azure,
		Name:    scName,
		Azure: &TierAzure{
			AccountName: accountName,
			AccountKey:  accountKey,
			Bucket:      bucket,

			// custom values
			Endpoint: endpoint,
			Prefix:   prefix,
			Region:   region,
		},
	}

	options := []AzureOptions{
		AzureEndpoint(endpoint),
		AzurePrefix(prefix),
		AzureRegion(region),
	}

	got, err := NewTierAzure(scName, accountName, accountKey, bucket, options...)
	if err != nil {
		t.Fatalf("Failed to create a custom azure tier %s", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got != want, got = %v want = %v", *got, *want)
	}
}

// TestGCSStorageClass tests GCSOptions helpers
func TestGCSStorageClass(t *testing.T) {
	scName := "test-gcs"
	credsJSON := []byte("test-creds-json")
	encodedCreds := base64.URLEncoding.EncodeToString(credsJSON)
	bucket, prefix := "testbucket", "testprefix"
	region := "us-west-2"

	want := &TierConfig{
		Version: TierConfigV1,
		Type:    GCS,
		Name:    scName,
		GCS: &TierGCS{
			Bucket: bucket,
			Creds:  encodedCreds,

			// custom values
			Endpoint: "https://storage.googleapis.com/",
			Prefix:   prefix,
			Region:   region,
		},
	}
	options := []GCSOptions{
		GCSRegion(region),
		GCSPrefix(prefix),
	}
	got, err := NewTierGCS(scName, credsJSON, bucket, options...)
	if err != nil {
		t.Fatalf("Failed to create a custom gcs tier %s", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got != want, got = %v want = %v", *got, *want)
	}
}
