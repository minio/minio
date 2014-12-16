package minio

import (
	"io"
	"log"
	"os"

	"github.com/codegangsta/cli"
)

func List(c *cli.Context) {
	config, err := parseInput(c)
	if err != nil {
		log.Fatal(err)
	}

	config.k = 10
	config.m = 6
	reader, err := erasureGetList(config, "")
	if err != nil {
		log.Fatal(err)
	}
	io.Copy(os.Stdout, reader)
}
