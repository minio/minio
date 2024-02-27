//go:build ignore
// +build ignore

//
// MinIO Object Storage (c) 2022 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/minio/madmin-go/v3"
)

func main() {
	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// API requests are secure (HTTPS) if secure=true and insecure (HTTP) otherwise.
	// New returns an MinIO Admin client object.
	madmClnt, err := madmin.New(os.Args[1], os.Args[2], os.Args[3], false)
	if err != nil {
		log.Fatalln(err)
	}

	opts := madmin.HealOpts{
		Recursive: true,                  // recursively heal all objects at 'prefix'
		Remove:    true,                  // remove content that has lost quorum and not recoverable
		ScanMode:  madmin.HealNormalScan, // by default do not do 'deep' scanning
	}

	start, _, err := madmClnt.Heal(context.Background(), "healing-rewrite-bucket", "", opts, "", false, false)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Healstart sequence ===")
	enc := json.NewEncoder(os.Stdout)
	if err = enc.Encode(&start); err != nil {
		log.Fatalln(err)
	}

	fmt.Println()
	for {
		_, status, err := madmClnt.Heal(context.Background(), "healing-rewrite-bucket", "", opts, start.ClientToken, false, false)
		if status.Summary == "finished" {
			fmt.Println("Healstatus on items ===")
			for _, item := range status.Items {
				if err = enc.Encode(&item); err != nil {
					log.Fatalln(err)
				}
			}
			break
		}
		if status.Summary == "stopped" {
			fmt.Println("Healstatus on items ===")
			fmt.Println("Heal failed with", status.FailureDetail)
			break
		}

		for _, item := range status.Items {
			if err = enc.Encode(&item); err != nil {
				log.Fatalln(err)
			}
		}

		time.Sleep(time.Second)
	}
}
