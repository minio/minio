package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/erasure"
)

func decode(c *cli.Context) {
	// check if minio-encode called without parameters
	if len(c.Args()) != 1 {
		cli.ShowCommandHelp(c, "decode")
		return
	}

	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}

	k := config.k
	m := config.m

	// check if output file exists, fail if so
	if _, err := os.Stat(config.output); !os.IsNotExist(err) {
		log.Fatal("Output file exists")
	}

	// get list of files
	var inputFiles []string
	if _, err := os.Stat(config.input + ".length"); os.IsNotExist(err) {
		err = nil
		chunkCount := 0
		for !os.IsNotExist(err) {
			_, err = os.Stat(config.input + "." + strconv.Itoa(chunkCount) + ".length")
			chunkCount += 1
		}
		chunkCount = chunkCount - 1
		inputFiles = make([]string, chunkCount)
		for i := 0; i < chunkCount; i++ {
			inputFiles[i] = config.input + "." + strconv.Itoa(i)
		}
	} else {
		inputFiles = []string{config.input}
	}

	// open file to write
	outputFile, err := os.OpenFile(config.output, os.O_CREATE|os.O_WRONLY, 0600)
	defer outputFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	for _, inputFile := range inputFiles {
		// get chunks
		chunks := make([][]byte, k+m)
		for i := 0; i < k+m; i++ {
			chunks[i], _ = ioutil.ReadFile(inputFile + "." + strconv.Itoa(i))
		}

		// get length
		lengthBytes, err := ioutil.ReadFile(inputFile + ".length")
		if err != nil {
			log.Fatal(err)
		}
		lengthString := string(lengthBytes)
		length, err := strconv.Atoi(lengthString)
		if err != nil {
			log.Fatal(err)
		}

		// set up encoder
		erasureParameters, _ := erasure.ParseEncoderParams(k, m, erasure.CAUCHY)

		// decode data
		decodedData, err := erasure.Decode(chunks, erasureParameters, length)
		if err != nil {
			log.Fatal(err)
		}
		// append decoded data
		length, err = outputFile.Write(decodedData)
		if err != nil {

			log.Fatal(err)
		}
	}
}
