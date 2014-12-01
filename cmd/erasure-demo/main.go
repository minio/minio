package main

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/codegangsta/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "erasure-demo"
	app.Usage = "erasure encode a byte stream"
	app.Commands = []cli.Command{
		{
			Name:   "encode",
			Usage:  "erasure encode a byte stream",
			Action: encode,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output,o",
					Value: "",
					Usage: "Output file",
				},
				cli.StringFlag{
					Name:  "protection-level",
					Value: "10,6",
					Usage: "data,parity",
				},
			},
		},
		{
			Name:   "decode",
			Usage:  "erasure decode a byte stream",
			Action: decode,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output,o",
					Value: "",
					Usage: "Output file",
				},
				cli.StringFlag{
					Name:  "protection-level",
					Value: "10,6",
					Usage: "data,parity",
				},
			},
		},
	}
	app.Run(os.Args)
}

// config representing cli input
type inputConfig struct {
	input     string
	output    string
	k         int
	m         int
	blockSize int
}

// parses input and returns an inputConfig with parsed input
func parseInput(c *cli.Context) (inputConfig, error) {
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
		return inputConfig{}, errors.New("Malformed input for protection-level")
	}

	k, err := strconv.Atoi(protectionLevelSplit[0])
	if err != nil {
		return inputConfig{}, err
	}

	m, err := strconv.Atoi(protectionLevelSplit[1])
	if err != nil {
		return inputConfig{}, err
	}

	return inputConfig{
		input:  inputFilePath,
		output: outputFilePath,
		k:      k,
		m:      m,
	}, nil
}
