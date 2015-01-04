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
	"io"
	"log"
	"os"
	"path"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/split"
	"github.com/minio-io/minio/pkg/strbyteconv"
)

var Options = []cli.Command{
	Split,
	Merge,
}

var Split = cli.Command{
	Name:        "split",
	Usage:       "Describes how large each split should be",
	Description: "",
	Action:      doFileSplit,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "size,s",
			Value: "2M",
			Usage: "",
		},
	},
}

var Merge = cli.Command{
	Name:        "merge",
	Usage:       "Describes how large each split should be",
	Description: "",
	Action:      doFileMerge,
}

func doFileSplit(c *cli.Context) {
	chunkSize, err := strbyteconv.StringToBytes(c.String("size"))
	if err != nil {
		log.Fatal(err)
	}
	err = split.SplitFileWithPrefix(c.Args().Get(0), chunkSize, c.Args().Get(1))
	if err != nil {
		// TODO cleanup?
		log.Fatal(err)
	}
}

func doFileMerge(c *cli.Context) {
	prefix := c.Args().Get(0)
	output := c.Args().Get(1)
	prefix = path.Clean(prefix)
	log.Println(path.Dir(prefix), path.Base(prefix))
	reader := split.JoinFiles(path.Dir(prefix), path.Base(prefix))
	file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	io.Copy(file, reader)
}
