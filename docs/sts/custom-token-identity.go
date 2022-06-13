//go:build ignore
// +build ignore

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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	// LDAP integrated Minio endpoint
	stsEndpoint string

	// token to use with AssumeRoleWithCustomToken
	token string

	// Role ARN to use
	roleArn string

	// Display credentials flag
	displayCreds bool

	// Credential expiry duration
	expiryDuration time.Duration

	// Bucket to list
	bucketToList string
)

func init() {
	flag.StringVar(&stsEndpoint, "sts-ep", "http://localhost:9000", "STS endpoint")
	flag.StringVar(&token, "t", "", "Token to use with AssumeRoleWithCustomToken STS API (required)")
	flag.StringVar(&roleArn, "r", "", "RoleARN to use with the request (required)")
	flag.BoolVar(&displayCreds, "d", false, "Only show generated credentials")
	flag.DurationVar(&expiryDuration, "e", 0, "Request a duration of validity for the generated credential")
	flag.StringVar(&bucketToList, "b", "mybucket", "Bucket to list (defaults to mybucket)")
}

func main() {
	flag.Parse()
	if token == "" || roleArn == "" {
		flag.PrintDefaults()
		return
	}

	// The credentials package in minio-go provides an interface to call the
	// AssumeRoleWithCustomToken STS API.

	var opts []cr.CustomTokenOpt
	if expiryDuration != 0 {
		opts = append(opts, cr.CustomTokenValidityOpt(expiryDuration))
	}

	// Initialize
	li, err := cr.NewCustomTokenCredentials(stsEndpoint, token, roleArn, opts...)
	if err != nil {
		log.Fatalf("Error initializing CustomToken Identity: %v", err)
	}

	v, err := li.Get()
	if err != nil {
		log.Fatalf("Error retrieving STS credentials: %v", err)
	}

	if displayCreds {
		fmt.Println("Only displaying credentials:")
		fmt.Println("AccessKeyID:", v.AccessKeyID)
		fmt.Println("SecretAccessKey:", v.SecretAccessKey)
		fmt.Println("SessionToken:", v.SessionToken)
		return
	}

	// Use generated credentials to authenticate with MinIO server
	stsEndpointURL, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatalf("Error parsing sts endpoint: %v", err)
	}
	copts := &minio.Options{
		Creds:  li,
		Secure: stsEndpointURL.Scheme == "https",
	}
	minioClient, err := minio.New(stsEndpointURL.Host, copts)
	if err != nil {
		log.Fatalf("Error initializing client: ", err)
	}

	// Use minIO Client object normally like the regular client.
	fmt.Printf("Calling list objects on bucket named `%s` with temp creds:\n===\n", bucketToList)
	objCh := minioClient.ListObjects(context.Background(), bucketToList, minio.ListObjectsOptions{})
	for obj := range objCh {
		if obj.Err != nil {
			log.Fatalf("Listing error: %v", obj.Err)
		}
		fmt.Printf("Key: %s\nSize: %d\nLast Modified: %s\n===\n", obj.Key, obj.Size, obj.LastModified)
	}
}
