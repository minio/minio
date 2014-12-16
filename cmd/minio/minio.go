package main

import (
	"os"

	"github.com/codegangsta/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "minio"
	app.Usage = "minio - object storage"
	app.Commands = []cli.Command{
		{
			Name:   "commit",
			Usage:  "provide higher protection for uploaded object",
			Action: commit,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: getObjectdir(".minio/erasure"),
					Usage: "",
				},
				cli.StringFlag{
					Name:  "staging",
					Value: getObjectdir(".minio/staging"),
					Usage: "",
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
			Name:   "get",
			Usage:  "get an object",
			Action: get,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: getObjectdir(".minio/erasure"),
					Usage: "",
				},
				cli.StringFlag{
					Name:  "block-size",
					Value: "1M",
					Usage: "Size of blocks. Examples: 1K, 1M, full",
				},
			},
		},
		{
			Name:   "add",
			Usage:  "add an object",
			Action: add,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "staging",
					Value: getObjectdir(".minio/staging"),
					Usage: "",
				},
				cli.StringFlag{
					Name:  "block-size",
					Value: "1M",
					Usage: "Size of blocks. Examples: 1K, 1M, full",
				},
			},
		},
		{
			Name:   "list",
			Usage:  "list objects",
			Action: list,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: getObjectdir(".minio/erasure"),
					Usage: "",
				},
			},
		},
	}
	app.Run(os.Args)
}
