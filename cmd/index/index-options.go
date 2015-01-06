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
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/go-patricia/patricia"
	"github.com/minio-io/minio/pkg/storage"
)

var Options = []cli.Command{
	Add,
	Get,
	List,
	Remove,
}

var Add = cli.Command{
	Name:        "add",
	Usage:       "",
	Description: "",
	Action:      doAdd,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Value: "objects.trie",
		},
	},
}

var Get = cli.Command{
	Name:        "get",
	Usage:       "",
	Description: "",
	Action:      doGet,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Value: "objects.trie",
		},
	},
}

var List = cli.Command{
	Name:        "list",
	Usage:       "",
	Description: "",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Value: "objects.trie",
		},
	},
	Action: doList,
}

var Remove = cli.Command{
	Name:        "remove",
	Usage:       "",
	Description: "",
	Action:      doRemove,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Value: "objects.trie",
		},
	},
}

func doAdd(c *cli.Context) {
	// register patricia for gob
	registerPatriciaWithGob()

	trie, err := readTrie(c.String("path"))
	if err != nil {
		log.Panic("err: ", trie, err)
	}
	log.Println("trie: ", trie, err)

	var object storage.ObjectDescription
	object.Name = c.Args().Get(1)
	object.Md5sum = "md5sum"
	object.Murmur3 = "murmur3"

	key := []byte(c.Args().Get(0))

	trie.Insert(key, object)
	log.Println("newTrie:", trie)
	saveTrie(c.String("path"), trie)
}

func doGet(c *cli.Context) {
	registerPatriciaWithGob()

	trie, err := readTrie(c.String("path"))
	if err != nil {
		log.Panic("err: ", trie, err)
	}
	description := trie.Get([]byte(c.Args().Get(0)))
	log.Println(trie)
	log.Println(description)

}
func doList(c *cli.Context) {
	registerPatriciaWithGob()
	trie, err := readTrie(c.String("path"))
	if err != nil {
		log.Panic("err: ", trie, err)
	}
	prefix := patricia.Prefix(c.Args().Get(0))
	trie.VisitSubtree(prefix, func(prefix patricia.Prefix, item patricia.Item) error {
		fmt.Println(string(prefix))
		return nil
	})
}

func doRemove(c *cli.Context) {
	registerPatriciaWithGob()
	trie, err := readTrie(c.String("path"))
	if err != nil {
		log.Panic("err: ", trie, err)
	}
	prefix := patricia.Prefix(c.Args().Get(0))
	trie.Delete(prefix)
	saveTrie(c.String("path"), trie)
}

func readTrie(path string) (*patricia.Trie, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return patricia.NewTrie(), nil
		} else {
			return nil, err
		}
	}
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	decoder := gob.NewDecoder(file)
	trie := patricia.NewTrie()
	if err = decoder.Decode(trie); err != nil {
		return nil, err
	}
	return trie, nil
}

func saveTrie(path string, trie *patricia.Trie) error {
	//	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC, 0600)
	//	if err != nil {
	//		return err
	//	}
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(trie); err != nil {
		log.Panic(err)
	}
	log.Println("marshalled trie:", buffer.Bytes())
	err := ioutil.WriteFile(path, buffer.Bytes(), 0600)
	return err
}

func registerPatriciaWithGob() {
	gob.Register(storage.ObjectDescription{})
	gob.Register(&patricia.SparseChildList{})
	gob.Register(&patricia.DenseChildList{})
}
