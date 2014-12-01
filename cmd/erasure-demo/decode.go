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
	// get chunks
	chunks := make([][]byte, k+m)
	for i := 0; i < k+m; i++ {
		chunks[i], _ = ioutil.ReadFile(config.input + "." + strconv.Itoa(i))
	}

	// get length
	lengthBytes, err := ioutil.ReadFile(config.input + ".length")
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

	// write decode data out
	if _, err := os.Stat(config.output); os.IsNotExist(err) {
		ioutil.WriteFile(config.output, decodedData, 0600)
	} else {
		log.Fatal("Output file already exists")
	}
}
