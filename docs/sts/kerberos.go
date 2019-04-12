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

	"github.com/minio/gokrb5/v7/config"
	"github.com/minio/minio-go"
	cr "github.com/minio/minio-go/pkg/credentials"
)

var (
	// Kerberized Minio endpoint
	stsEndpoint = "http://localhost:9000"

	// Kerberos user credentials
	userPrincipal = "aditya"
	password      = "abcdef"

	// Kerberos realm
	realm = "CHORKE.ORG"

	// Minio service Kerberos principal
	principal = "minio/myminio.com"
)

// Load up Kerberos client configuration from customly located
// kerberos client config file. If your system is configured as a
// Kerberos client, this function is not needed.
func getKrbConfig() *config.Config {
	cfg, err := config.Load("/tmp/krb5.conf")
	if err != nil {
		log.Fatalf("Config err: %v", err)
	}
	return cfg
}

func main() {

	// If client machine is configured as a Kerberos client, just
	// pass nil instead of `getKrbConfig()` below.
	ki, err := cr.NewKerberosIdentity(stsEndpoint, getKrbConfig(), userPrincipal, password, realm, principal)
	if err != nil {
		log.Fatalf("INIT Err: %v", err)
	}

	v, err := ki.Get()
	if err != nil {
		log.Fatalf("GET Err: %v", err)
	}
	fmt.Printf("%#v\n", v)

	// Temporary credentials would be present in `ki`. Before we
	// can use them, we need to make sure an access policy is
	// setup for the Kerberos user on Minio. This is done by
	// invoking an admin API at
	// github.com/minio/minio/pkg/madmin.SetKrbUserPolicy() - this
	// is done by an administrator.
	//
	// Assuming this is done (with an appropriate policy), the
	// following client access using the temporary credentials is
	// expected to work.
	//
	// The policy assignment call above can be done using `mc`:
	//
	//      mc admin user policy --sts-kerberos aditya@CHORKE.ORG readwrite

	minioClient, err := minio.NewWithCredentials("localhost:9000", ki, false, "")
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
