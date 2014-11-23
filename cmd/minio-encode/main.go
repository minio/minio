package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/erasure"
)

func main() {
	app := cli.NewApp()
	app.Name = "minio-encode"
	app.Usage = "erasure encode a byte stream"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "input,i",
			Value: "",
			Usage: "Input file",
		},
		cli.StringFlag{
			Name:  "output,o",
			Value: "",
			Usage: "Output file",
		},
	}
	app.Action = func(c *cli.Context) {
		// check if minio-encode called without parameters
		if len(c.Args()) == 1 {
			cli.ShowAppHelp(c)
		}

		// get input path
		if c.String("input") == "" {
			log.Fatal("No input specified")
		}
		inputFilePath := c.String("input")

		// get output path
		outputFilePath := inputFilePath
		if c.String("output") != "" {
			outputFilePath = c.String("output")
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
		erasureParameters, _ := erasure.ValidateParams(10, 5, 8, erasure.CAUCHY)
		encoder := erasure.NewEncoder(erasureParameters)

		// encode data
		encodedData, _ := encoder.Encode(input)

		// write encoded data out
		for key, data := range encodedData {
			ioutil.WriteFile(outputFilePath+"."+strconv.Itoa(key), data, 0600)
		}
	}
	app.Run(os.Args)
}
