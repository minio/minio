package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/erasure"
)

func encode(c *cli.Context) {
	// check if minio-encode called without parameters
	if len(c.Args()) != 1 {
		cli.ShowCommandHelp(c, "encode")
		return
	}

	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}

	// get file
	inputFile, err := os.Open(config.input)
	if err != nil {
		log.Fatal(err)
	}

	// read file
	input, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatal(err)
	}

	// set up encoder
	erasureParameters, _ := erasure.ParseEncoderParams(config.k, config.m, erasure.CAUCHY)
	// encode data
	encodedData, length := erasure.Encode(input, erasureParameters)

	// write encoded data out
	for key, data := range encodedData {
		ioutil.WriteFile(config.output+"."+strconv.Itoa(key), data, 0600)
	}
	ioutil.WriteFile(config.output+".length", []byte(strconv.Itoa(length)), 0600)
}
