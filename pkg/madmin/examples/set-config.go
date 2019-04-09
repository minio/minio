// +build ignore

/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/minio/minio/pkg/madmin"
)

var configJSON = []byte(`{
	"version": "13",
	"credential": {
		"accessKey": "minio",
		"secretKey": "minio123"
	},
	"region": "us-west-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "fatal"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": ""
		}
	},
	"notify": {
		"amqp": {
			"1": {
				"enable": false,
				"url": "",
				"exchange": "",
				"routingKey": "",
				"exchangeType": "",
				"mandatory": false,
				"immediate": false,
				"durable": false,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
		},
		"nats": {
			"1": {
				"enable": false,
				"address": "",
				"subject": "",
				"username": "",
				"password": "",
				"token": "",
				"secure": false,
				"pingInterval": 0,
				"streaming": {
					"enable": false,
					"clusterID": "",
					"async": false,
					"maxPubAcksInflight": 0
				}
			}
		},
		"elasticsearch": {
			"1": {
				"enable": false,
				"url": "",
				"index": ""
			}
		},
		"redis": {
			"1": {
				"enable": false,
				"address": "",
				"password": "",
				"key": ""
			}
		},
		"postgresql": {
			"1": {
				"enable": false,
				"connectionString": "",
				"table": "",
				"host": "",
				"port": "",
				"user": "",
				"password": "",
				"database": ""
			}
		},
		"kafka": {
			"1": {
				"enable": false,
				"brokers": null,
				"topic": ""
			}
		},
		"webhook": {
			"1": {
				"enable": false,
				"endpoint": ""
			}
		}
	}
}`)

func main() {
	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY are
	// dummy values, please replace them with original values.

	// API requests are secure (HTTPS) if secure=true and insecure (HTTPS) otherwise.
	// New returns an MinIO Admin client object.
	madmClnt, err := madmin.New("your-minio.example.com:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
	if err != nil {
		log.Fatalln(err)
	}

	result, err := madmClnt.SetConfig(bytes.NewReader(configJSON))
	if err != nil {
		log.Fatalln(err)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "\t")
	err = enc.Encode(result)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("SetConfig: ", string(buf.Bytes()))
}
