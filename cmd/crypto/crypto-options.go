/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/crypto/md5"
	"github.com/minio-io/minio/pkgs/crypto/sha1"
	"github.com/minio-io/minio/pkgs/crypto/sha256"
	"github.com/minio-io/minio/pkgs/crypto/sha512"
)

var Options = []cli.Command{
	Md5sum,
	Sha1sum,
	// Sha1sumFast, // not working
	Sha256sum,
	Sha512sum,
}

var Md5sum = cli.Command{
	Name:  "md5sum",
	Usage: "",
	Description: `
`,
	Action: doMd5sum,
}

var Sha1sum = cli.Command{
	Name:  "sha1sum",
	Usage: "",
	Description: `
`,
	Action: doSha1sum,
}

var Sha1sumFast = cli.Command{
	Name:  "sha1sum-fast",
	Usage: "",
	Description: `
`,
	Action: doSha1sumFast,
}

var Sha256sum = cli.Command{
	Name:  "sha256sum",
	Usage: "",
	Description: `
`,
	Action: doSha256sum,
}

var Sha512sum = cli.Command{
	Name:  "sha512sum",
	Usage: "",
	Description: `
`,
	Action: doSha512sum,
}

func doMd5sum(c *cli.Context) {
	hash, err := md5.Sum(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%x", hash)
}

func doSha1sum(c *cli.Context) {
	hash, err := sha1.Sum(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%x", hash)
}

func doSha1sumFast(c *cli.Context) {
	buffer, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	hash, err := sha1.Sha1(buffer)
	if err != nil {
		log.Fatal(err)
	}
	var bytesBuffer bytes.Buffer
	binary.Write(&bytesBuffer, binary.LittleEndian, hash)
	fmt.Printf("%x", bytesBuffer.Bytes())
}

func doSha256sum(c *cli.Context) {
	hash, err := sha256.Sum(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%x", hash)
}

func doSha512sum(c *cli.Context) {
	hash, err := sha512.Sum(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%x", hash)
}
