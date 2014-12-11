package main

import (
	"io"
	"log"
	"os"

	"github.com/codegangsta/cli"
)

func get(c *cli.Context) {
	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}
	var objectReader io.Reader
	objectName := c.Args().Get(0)

	switch config.storageDriver {
	case "fs":
		{
			if len(objectName) == 0 {
				if objectReader, err = fsGetList(config); err != nil {
					log.Fatal(err)
				}
			} else {
				if objectReader, err = fsGet(config, objectName); err != nil {
					log.Fatal(err)
				}
			}
		}
	case "erasure":
		{
			if objectReader, err = erasureGet(config, objectName); err != nil {
				log.Fatal(err)
			}
		}
	default:
		{
			log.Fatal("Unknown driver")
		}
	}
	io.Copy(os.Stdout, objectReader)
}
