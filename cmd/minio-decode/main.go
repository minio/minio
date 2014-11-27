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
	app.Name = "minio-decode"
	app.Usage = "erasure decode a byte stream"
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
		erasureParameters, _ := erasure.ValidateParams(k, m, 8, erasure.CAUCHY)

		// decode data
		decodedData, err := erasure.Decode(chunks, erasureParameters, length)
		if err != nil {
			log.Fatal(err)
		}

		// write decode data out
		ioutil.WriteFile(outputFilePath, decodedData, 0600)
	}
	app.Run(os.Args)
}
