package main

import (
	"log"
	"os"
	"path"

	"github.com/codegangsta/cli"
)

func put(c *cli.Context) {
	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}
	var filePath, objectName string
	switch len(c.Args()) {
	case 1:
		objectName = path.Base(c.Args().Get(0))
		filePath = c.Args().Get(0)
	case 2:
		objectName = c.Args().Get(0)
		filePath = c.Args().Get(1)
	default:
		log.Fatal("Please specify a valid object name \n # erasure-demo put [OBJECTNAME] [FILENAME]")
	}
	inputFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	switch config.storageDriver {
	case "fs":
		{
			if err := fsPut(config, objectName, inputFile); err != nil {
				log.Fatal(err)
			}
		}
	case "erasure":
		{
			if err := erasurePut(config, objectName, inputFile); err != nil {
				log.Fatal(err)
			}
		}
	default:
		{
			log.Fatal("Unknown driver")
		}
	}
}
