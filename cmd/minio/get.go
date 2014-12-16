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
	var objectName string
	var objectReader io.Reader
	switch len(c.Args()) {
	case 1:
		objectName = c.Args().Get(0)
	default:
		log.Fatal("Please specify a valid object name \n # erasure-demo get [OBJECTNAME]")
	}

	if objectReader, err = erasureGet(config, objectName); err != nil {
		log.Fatal(err)
	}

	io.Copy(os.Stdout, objectReader)
}
