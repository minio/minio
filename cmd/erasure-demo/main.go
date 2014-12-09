package main

import (
	"errors"
	"log"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/strbyteconv"
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
				cli.StringFlag{
					Name:  "block-size",
					Value: "1M",
					Usage: "Size of blocks. Examples: 1K, 1M, full",
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
		{
			Name:   "get",
			Usage:  "get an object",
			Action: get,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: getMinioDir(),
					Usage: "",
				},
				cli.StringFlag{
					Name:  "driver",
					Value: "fs",
					Usage: "fs",
				},
			},
		},
		{
			Name:   "put",
			Usage:  "put an object",
			Action: put,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: getMinioDir(),
					Usage: "",
				},
				cli.StringFlag{
					Name:  "driver",
					Value: "fs",
					Usage: "fs",
				},
			},
		},
	}
	app.Run(os.Args)
}

// config representing cli input
type inputConfig struct {
	input         string
	output        string
	k             int
	m             int
	blockSize     uint64
	rootDir       string
	storageDriver string
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

	var k, m int
	if c.String("protection-level") != "" {
		protectionLevel := c.String("protection-level")
		protectionLevelSplit := strings.Split(protectionLevel, ",")
		if len(protectionLevelSplit) != 2 {
			return inputConfig{}, errors.New("Malformed input for protection-level")
		}

		var err error
		k, err = strconv.Atoi(protectionLevelSplit[0])
		if err != nil {
			return inputConfig{}, err
		}
		m, err = strconv.Atoi(protectionLevelSplit[1])
		if err != nil {
			return inputConfig{}, err
		}
	}

	var blockSize uint64
	blockSize = 0
	if c.String("block-size") != "" {
		if c.String("block-size") != "full" {
			var err error
			blockSize, err = strbyteconv.StringToBytes(c.String("block-size"))
			if err != nil {
				return inputConfig{}, err
			}
		}
	}

	var rootDir string
	if c.String("root") != "" {
		rootDir = c.String("root")
	}

	var storageDriver string
	if c.String("driver") != "" {
		storageDriver = c.String("driver")
	}

	config := inputConfig{
		input:         inputFilePath,
		output:        outputFilePath,
		k:             k,
		m:             m,
		blockSize:     blockSize,
		rootDir:       rootDir,
		storageDriver: storageDriver,
	}
	return config, nil
}

func getMinioDir() string {
	user, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	homePath := user.HomeDir
	minioPath := path.Join(homePath, ".minio")
	err = _initMinioDir(minioPath)
	if err != nil {
		log.Fatal(err)
	}
	return minioPath
}

func _initMinioDir(dirPath string) error {
	_, err := os.Lstat(dirPath)
	if err != nil {
		log.Printf("%s not found, creating a new-one for the first time",
			dirPath)
		err = os.Mkdir(dirPath, 0700)
		if err != nil {
			return err
		}
	}
	return nil
}
