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
	switch config.storageDriver {
	case "fs":
		{
			if objectReader, err = fsGet(config, c.Args().Get(0)); err != nil {
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

func put(c *cli.Context) {
	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}
	filePath := c.Args().Get(1)
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
