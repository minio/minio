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
	"flag"
	"fmt"
	"log"
	"net/url"

	miniogo "github.com/minio/minio-go/v6"
	cr "github.com/minio/minio-go/v6/pkg/credentials"
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
	li, err := cr.NewLDAPIdentity(stsEndpoint, ldapUsername, ldapPassword)
	if err != nil {
		log.Fatalf("INIT Err: %v", err)
	}

	// Generate temporary STS credentials
	v, err := li.Get()
	if err != nil {
		log.Fatalf("GET Err: %v", err)
	}
	fmt.Printf("%#v\n", v)

	stsEndpointUrl, err := url.Parse(stsEndpoint)
	if err != nil {
		log.Fatalf("Err: %v", err)
	}

	secure := false
	if stsEndpointUrl.Scheme == "https" {
		secure = true
	}

	// Use generated credentials to authenticate with MinIO server
	minioClient, err := miniogo.NewWithCredentials(stsEndpointUrl.Host, li, secure, "")
	if err != nil {
		log.Fatalln(err)
	}

	// Use minIO Client object normally like the regular client.
	fmt.Println("Calling list buckets with temp creds:")
	b, err := minioClient.ListBuckets()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(b)
}
