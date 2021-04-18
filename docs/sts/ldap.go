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
	"log"
	"net/url"

	"github.com/minio/minio-go/v7"
	cr "github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	// LDAP integrated Minio endpoint
	stsEndpoint string

	// LDAP credentials
	ldapUsername string
	ldapPassword string
)

func init() {
	flag.StringVar(&stsEndpoint, "sts-ep", "http://localhost:9000", "STS endpoint")
	flag.StringVar(&ldapUsername, "u", "", "AD/LDAP Username")
	flag.StringVar(&ldapPassword, "p", "", "AD/LDAP Password")
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
	li, _ := cr.NewLDAPIdentity(stsEndpoint, ldapUsername, ldapPassword)

	stsEndpointURL, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatalf("Err: %v", err)
	}

	opts := &minio.Options{
		Creds:  li,
		Secure: stsEndpointURL.Scheme == "https",
	}

	fmt.Println(li.Get())
	// Use generated credentials to authenticate with MinIO server
	minioClient, err := minio.New(stsEndpointURL.Host, opts)
	if err != nil {
		log.Fatalln(err)
	}

	// Use minIO Client object normally like the regular client.
	fmt.Println("Calling list objects with temp creds: ")
	objCh := minioClient.ListObjects(context.Background(), ldapUsername, minio.ListObjectsOptions{})
	for obj := range objCh {
		if obj.Err != nil {
			if err != nil {
				log.Fatalln(err)
			}
		}
		fmt.Println(obj)
	}
}
