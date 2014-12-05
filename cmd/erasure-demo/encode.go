package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/erasure"
	"github.com/minio-io/minio/pkgs/split"
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
	defer inputFile.Close()
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
	if config.blockSize == 0 {
		encodedData, length := erasure.Encode(input, erasureParameters)
		for key, data := range encodedData {
			ioutil.WriteFile(config.output+"."+strconv.Itoa(key), data, 0600)
			ioutil.WriteFile(config.output+".length", []byte(strconv.Itoa(length)), 0600)
		}
	} else {
		chunkCount := 0
		splitChannel := make(chan split.SplitMessage)
		inputReader := bytes.NewReader(input)
		go split.SplitStream(inputReader, config.blockSize, splitChannel)
		for chunk := range splitChannel {
			if chunk.Err != nil {
				log.Fatal(chunk.Err)
			}
			encodedData, length := erasure.Encode(chunk.Data, erasureParameters)
			for key, data := range encodedData {
				ioutil.WriteFile(config.output+"."+strconv.Itoa(chunkCount)+"."+strconv.Itoa(key), data, 0600)
				ioutil.WriteFile(config.output+"."+strconv.Itoa(chunkCount)+".length", []byte(strconv.Itoa(length)), 0600)
			}
			chunkCount += 1
		}
	}
}
