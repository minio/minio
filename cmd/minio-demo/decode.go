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

func decode(c *cli.Context) {
	// check if minio-encode called without parameters
	if len(c.Args()) != 1 {
		cli.ShowCommandHelp(c, "decode")
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

	// get chunks
	chunks := make([][]byte, k+m)
	for i := 0; i < k+m; i++ {
		chunks[i], _ = ioutil.ReadFile(inputFilePath + "." + strconv.Itoa(i))
	}

	// get length
	lengthBytes, err := ioutil.ReadFile(inputFilePath + ".length")
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
	if _, err := os.Stat(outputFilePath); os.IsNotExist(err) {
		ioutil.WriteFile(outputFilePath, decodedData, 0600)
	} else {
		log.Fatal("Output file already exists")
	}
}
