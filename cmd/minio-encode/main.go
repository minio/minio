package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/erasure"
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
		cli.IntFlag{
			Name:  "k",
			Value: 10,
			Usage: "k value of encoder parameters",
		},
		cli.IntFlag{
			Name:  "m",
			Value: 5,
			Usage: "m value of encoder parameters",
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

		k := c.Int("k")
		m := c.Int("m")

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
		erasureParameters, _ := erasure.ValidateParams(k, m, 8, erasure.CAUCHY)
		// encode data
		encodedData, length := erasure.Encode(input, erasureParameters)

		// write encoded data out
		for key, data := range encodedData {
			ioutil.WriteFile(outputFilePath+"."+strconv.Itoa(key), data, 0600)
		}
		ioutil.WriteFile(outputFilePath+".length", []byte(strconv.Itoa(length)), 0600)
	}
	app.Run(os.Args)
}
