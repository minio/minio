//+build ignore

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/minio/minio/cmd"
)

func main() {
	flag.Parse()
	args := flag.Args()
	for _, arg := range args {
		b, err := ioutil.ReadFile(arg)
		if err != nil {
			log.Fatal(err)
		}
		meta, err := cmd.ExternLoadMetaV2(b)
		if err != nil {
			log.Fatal(err)
		}
		b, err = json.MarshalIndent(meta, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	}
}
