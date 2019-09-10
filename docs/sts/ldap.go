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
	"fmt"
	"log"

	miniogo "github.com/minio/minio-go"
	cr "github.com/minio/minio-go/pkg/credentials"
)

var (
	// LDAP integrated Minio endpoint
	stsEndpoint = "http://localhost:9000"

	// LDAP credentials
	ldapUsername = "ldapuser"
	ldapPassword = "ldapsecret"
)

func main() {

	// If client machine is configured as a Kerberos client, just
	// pass nil instead of `getKrbConfig()` below.
	li, err := cr.NewLDAPIdentity(stsEndpoint, ldapUsername, ldapPassword)
	if err != nil {
		log.Fatalf("INIT Err: %v", err)
	}

	v, err := li.Get()
	if err != nil {
		log.Fatalf("GET Err: %v", err)
	}
	fmt.Printf("%#v\n", v)

	minioClient, err := miniogo.NewWithCredentials("localhost:9000", li, false, "")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("Calling list buckets with temp creds:")
	b, err := minioClient.ListBuckets()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(b)
}
