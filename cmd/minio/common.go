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
	"github.com/minio-io/minio/pkg/strbyteconv"
)

// config representing cli input
type inputConfig struct {
	k          int
	m          int
	blockSize  uint64
	rootDir    string
	stagingDir string
}

// parses input and returns an inputConfig with parsed input
func parseInput(c *cli.Context) (inputConfig, error) {
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

	var stagingDir string
	if c.String("staging") != "" {
		stagingDir = c.String("staging")
	}

	config := inputConfig{
		k:          k,
		m:          m,
		blockSize:  blockSize,
		rootDir:    rootDir,
		stagingDir: stagingDir,
	}
	return config, nil
}

func getObjectdir(basename string) string {
	user, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	homePath := user.HomeDir
	minioPath := path.Join(homePath, basename)
	err = _initDir(minioPath)
	if err != nil {
		log.Fatal(err)
	}
	return minioPath
}

func _initDir(dirPath string) error {
	_, err := os.Lstat(dirPath)
	if err != nil {
		log.Printf("%s not found, creating a new-one for the first time",
			dirPath)
		err = os.MkdirAll(dirPath, 0700)
		if err != nil {
			return err
		}
	}
	return nil
}
