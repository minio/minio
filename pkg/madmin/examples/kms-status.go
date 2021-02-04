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
 *
 */

package main

import (
	"context"
	"log"

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

	status, err := madmClnt.GetKeyStatus(context.Background(), "") // empty string refers to the default master key
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Key: %s\n", status.KeyID)
	if status.EncryptionErr == "" {
		log.Println("\t • Encryption ✔")
	} else {
		log.Printf("\t • Encryption failed: %s\n", status.EncryptionErr)
	}
	if status.UpdateErr == "" {
		log.Println("\t • Re-wrap ✔")
	} else {
		log.Printf("\t • Re-wrap failed: %s\n", status.UpdateErr)
	}
	if status.DecryptionErr == "" {
		log.Println("\t • Decryption ✔")
	} else {
		log.Printf("\t •  Decryption failed: %s\n", status.DecryptionErr)
	}
}
