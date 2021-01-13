/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

package madmin

import (
	"fmt"
	"log"
)

func ExampleTransitionStorageClassS3() {
	simpleS3SC, err := NewTransitionStorageClassS3("simple-s3", "accessKey", "secretKey", "testbucket")
	if err != nil {
		log.Fatalln(err, "Failed to create s3 backed storage-class")
	}
	fmt.Println(simpleS3SC)

	fullyCustomS3SC, err := NewTransitionStorageClassS3("my-s3-infreq", "accessKey", "secretKey", "testbucket",
		S3Endpoint("https://s3.amazonaws.com"), S3Prefix("testprefix"), S3Region("us-west-1"), S3StorageClass("S3_IA"))
	if err != nil {
		log.Fatalln(err, "Failed to create s3 storage-class")
	}
	fmt.Println(fullyCustomS3SC)
}

func ExampleTransitionStorageClassAzure() {
	simpleAzSC, err := NewTransitionStorageClassAzure("simple-az", "accessKey", "secretKey", "testbucket")
	if err != nil {
		log.Fatalln(err, "Failed to create azure backed storage-class")
	}
	fmt.Println(simpleAzSC)

	fullyCustomAzSC, err := NewTransitionStorageClassAzure("simple-az", "accessKey", "secretKey", "testbucket", AzureEndpoint("http://blob.core.windows.net"), AzurePrefix("testprefix"))
	if err != nil {
		log.Fatalln(err, "Failed to create azure backed storage-class")
	}
	fmt.Println(fullyCustomAzSC)
}
