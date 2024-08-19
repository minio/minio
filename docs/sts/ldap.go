//go:build ignore
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
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	// LDAP integrated Minio endpoint
	stsEndpoint string

	// LDAP credentials
	ldapUsername string
	ldapPassword string

	// Display credentials flag
	displayCreds bool

	// Credential expiry duration
	expiryDuration time.Duration

	// Bucket to list
	bucketToList string

	// Session policy file
	sessionPolicyFile string
)

func init() {
	flag.StringVar(&stsEndpoint, "sts-ep", "http://localhost:9000", "STS endpoint")
	flag.StringVar(&ldapUsername, "u", "", "AD/LDAP Username")
	flag.StringVar(&ldapPassword, "p", "", "AD/LDAP Password")
	flag.BoolVar(&displayCreds, "d", false, "Only show generated credentials")
	flag.DurationVar(&expiryDuration, "e", 0, "Request a duration of validity for the generated credential")
	flag.StringVar(&bucketToList, "b", "", "Bucket to list (defaults to ldap username)")
	flag.StringVar(&sessionPolicyFile, "s", "", "File containing session policy to apply to the STS request")
}

func main() {
	flag.Parse()
	if ldapUsername == "" || ldapPassword == "" {
		flag.PrintDefaults()
		return
	}

	// The credentials package in minio-go provides an interface to call the
	// LDAP STS API.

	// Initialize LDAP credentials
	var ldapOpts []cr.LDAPIdentityOpt
	if sessionPolicyFile != "" {
		var policy string
		if f, err := os.Open(sessionPolicyFile); err != nil {
			log.Fatalf("Unable to open session policy file %s: %v", sessionPolicyFile, err)
		} else {
			bs, err := io.ReadAll(f)
			if err != nil {
				log.Fatalf("Error reading session policy file: %v", err)
			}
			policy = string(bs)
		}
		ldapOpts = append(ldapOpts, cr.LDAPIdentityPolicyOpt(policy))
	}
	if expiryDuration != 0 {
		ldapOpts = append(ldapOpts, cr.LDAPIdentityExpiryOpt(expiryDuration))
	}
	li, err := cr.NewLDAPIdentity(stsEndpoint, ldapUsername, ldapPassword, ldapOpts...)
	if err != nil {
		log.Fatalf("Error initializing LDAP Identity: %v", err)
	}

	stsEndpointURL, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatalf("Error parsing sts endpoint: %v", err)
	}

	opts := &minio.Options{
		Creds:  li,
		Secure: stsEndpointURL.Scheme == "https",
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
	minioClient, err := minio.New(stsEndpointURL.Host, opts)
	if err != nil {
		log.Fatalf("Error initializing client: %v", err)
	}

	// Use minIO Client object normally like the regular client.
	if bucketToList == "" {
		bucketToList = ldapUsername
	}
	fmt.Printf("Calling list objects on bucket named `%s` with temp creds:\n===\n", bucketToList)
	objCh := minioClient.ListObjects(context.Background(), bucketToList, minio.ListObjectsOptions{})
	for obj := range objCh {
		if obj.Err != nil {
			log.Fatalf("Listing error: %v", obj.Err)
		}
		fmt.Printf("Key: %s\nSize: %d\nLast Modified: %s\n===\n", obj.Key, obj.Size, obj.LastModified)
	}
}
