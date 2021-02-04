// +build ignore

/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
