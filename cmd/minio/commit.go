package main

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/codegangsta/cli"
)

func cleanupStagingDir(stagingDir string) {
	filelist, err := ioutil.ReadDir(stagingDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range filelist {
		_file := path.Join(stagingDir, file.Name())
		if err := os.Remove(_file); err != nil {
			log.Fatal(err)
		}
	}
}

func commit(c *cli.Context) {
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

	// Cleanup stagingDir
	cleanupStagingDir(config.stagingDir)
}
