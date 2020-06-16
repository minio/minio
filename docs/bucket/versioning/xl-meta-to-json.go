package main

import (
	"io"
	"os"

	"github.com/minio/cli"
	"github.com/tinylib/msgp/msgp"
)

func main() {
	app := cli.NewApp()
	app.Copyright = "MinIO, Inc."
	app.Usage = "xl.meta to JSON"
	app.Version = "0.0.1"
	app.HideHelpCommand = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Usage: "path to xl.meta file",
			Name:  "f, file",
		},
	}

	app.Action = func(c *cli.Context) error {
		r, err := os.Open(c.String("file"))
		if err != nil {
			return err
		}
		r.Seek(8, io.SeekStart)
		defer r.Close()
		_, err = msgp.CopyToJSON(os.Stdout, r)
		return err
	}
	app.Run(os.Args)
}
