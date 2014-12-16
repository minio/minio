package main

import (
	"log"

	"github.com/codegangsta/cli"
)

func encode(c *cli.Context) {
	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}
	var objectName string
	switch len(c.Args()) {
	case 1:
		objectName = c.Args().Get(0)
	default:
		log.Fatal("Please specify a valid object name \n # erasure-demo encode [OBJECTNAME]")
	}

	// Get from staging area
	stagingConfig := config
	stagingConfig.k = 2
	stagingConfig.m = 1
	stagingConfig.rootDir = config.stagingDir
	reader, err := erasureGet(stagingConfig, objectName)
	if err != nil {
		log.Fatal(err)
	}

	// Increase parity to user defined or default 10,6
	err = erasurePut(config, objectName, reader)
	if err != nil {
		log.Fatal(err)
	}
}
