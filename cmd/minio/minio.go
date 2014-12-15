package main

import (
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkgs/minio"
)

func main() {
	app := cli.NewApp()
	app.Name = "minio"
	app.Usage = "minio - object storage"
	app.Commands = []cli.Command{
		{
			Name:   "encode",
			Usage:  "erasure encode a byte stream",
			Action: minio.Encode,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: minio.Getobjectdir(".minio/erasure"),
					Usage: "",
				},
				cli.StringFlag{
					Name:  "staging",
					Value: minio.Getobjectdir(".minio/staging"),
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
			Action: minio.Get,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: minio.Getobjectdir(".minio/erasure"),
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
			Name:   "put",
			Usage:  "put an object",
			Action: minio.Put,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "staging",
					Value: minio.Getobjectdir(".minio/staging"),
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
			Action: minio.List,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "root",
					Value: minio.Getobjectdir(".minio/erasure"),
					Usage: "",
				},
			},
		},
	}
	app.Run(os.Args)
}
