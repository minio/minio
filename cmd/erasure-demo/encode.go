package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/erasure"
)

func encode(c *cli.Context) {
	// check if minio-encode called without parameters
	if len(c.Args()) != 1 {
		cli.ShowCommandHelp(c, "encode")
		return
	}

	// get input path
	inputFilePath := c.Args().Get(0)

	// get output path
	outputFilePath := inputFilePath
	if c.String("output") != "" {
		outputFilePath = c.String("output")
	}

	protectionLevel := c.String("protection-level")
	protectionLevelSplit := strings.Split(protectionLevel, ",")
	if len(protectionLevelSplit) != 2 {
		log.Fatal("Malformed input for protection-level")
	}

	k, err := strconv.Atoi(protectionLevelSplit[0])
	if err != nil {
		log.Fatal(err)
	}

	m, err := strconv.Atoi(protectionLevelSplit[1])
	if err != nil {
		log.Fatal(err)
	}

	// get file
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatal(err)
	}

	// read file
	input, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatal(err)
	}

	// set up encoder
	erasureParameters, _ := erasure.ParseEncoderParams(k, m, erasure.CAUCHY)
	// encode data
	encodedData, length := erasure.Encode(input, erasureParameters)

	// write encoded data out
	for key, data := range encodedData {
		ioutil.WriteFile(outputFilePath+"."+strconv.Itoa(key), data, 0600)
	}
	ioutil.WriteFile(outputFilePath+".length", []byte(strconv.Itoa(length)), 0600)
}
