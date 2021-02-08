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
 *
 */

package madmin

import (
	"encoding/base64"
	"fmt"
	"log"
	"testing"
)

func ExampleTransitionTierS3() {
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

func ExampleTransitionTierAzure() {
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

func ExampleTransitionTierGCS() {
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
	want := &TierS3{
		Name:      scName,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,

		// custom values
		Endpoint:     endpoint,
		Prefix:       prefix,
		Region:       region,
		StorageClass: storageClass,
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

	if *got != *want {
		t.Fatalf("got != want, got = %v want = %v", got, want)
	}
}

// TestAzTier tests AzureOptions helpers
func TestAzTier(t *testing.T) {
	scName := "test-az"
	endpoint := "https://myazure.com"
	accessKey, secretKey := "accessKey", "secretKey"
	bucket, prefix := "testbucket", "testprefix"
	region := "us-east-1"
	want := &TierAzure{
		Name:      scName,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,

		// custom values
		Endpoint: endpoint,
		Prefix:   prefix,
		Region:   region,
	}
	options := []AzureOptions{
		AzureEndpoint(endpoint),
		AzurePrefix(prefix),
		AzureRegion(region),
	}
	got, err := NewTierAzure(scName, accessKey, secretKey, bucket, options...)
	if err != nil {
		t.Fatalf("Failed to create a custom azure tier %s", err)
	}

	if *got != *want {
		t.Fatalf("got != want, got = %v want = %v", got, want)
	}
}

// TestGCSStorageClass tests GCSOptions helpers
func TestGCSStorageClass(t *testing.T) {
	scName := "test-gcs"
	credsJSON := []byte("test-creds-json")
	encodedCreds := base64.URLEncoding.EncodeToString(credsJSON)
	bucket, prefix := "testbucket", "testprefix"
	region := "us-west-2"
	want := &TierGCS{
		Name:   scName,
		Bucket: bucket,
		Creds:  encodedCreds,

		// custom values
		Endpoint: "https://storage.googleapis.com/",
		Prefix:   prefix,
		Region:   region,
	}
	options := []GCSOptions{
		GCSRegion(region),
		GCSPrefix(prefix),
	}
	got, err := NewTierGCS(scName, credsJSON, bucket, options...)
	if err != nil {
		t.Fatalf("Failed to create a custom gcs tier %s", err)
	}

	if *got != *want {
		t.Fatalf("got != want, got = %v want = %v", got, want)
	}
}
