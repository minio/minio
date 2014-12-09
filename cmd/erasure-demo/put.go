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
	filePath := c.Args().Get(0)
	if len(filePath) == 0 {
		log.Fatal("Please specify a valid object name \n # erasure-demo put <OBJECTNAME>")
	}
	inputFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	switch config.storageDriver {
	case "fs":
		{
			fsPut(config, c.Args().Get(0), inputFile)
		}
	default:
		{
			log.Fatal("Unknown driver")
		}
	}
}
