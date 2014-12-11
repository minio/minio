package main

import (
	"log"
	"os"

	"github.com/codegangsta/cli"
)

func put(c *cli.Context) {
	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}
	objectPath := c.Args().Get(0)
	var filePath string
	switch len(c.Args()) {
	case 1:
		filePath = objectPath
	case 2:
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
			if err := fsPut(config, c.Args().Get(0), inputFile); err != nil {
				log.Fatal(err)
			}
		}
	case "erasure":
		{
			if err := erasurePut(config, c.Args().Get(0), inputFile); err != nil {
				log.Fatal(err)
			}
		}
	default:
		{
			log.Fatal("Unknown driver")
		}
	}
}
