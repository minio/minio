package main

import (
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/server"
)

func parseInput(c *cli.Context) {
	tls := c.Bool("tls")
	certFile := c.String("cert")
	keyFile := c.String("key")
	inmemory := c.Bool("inmemory")
	server.Start(":8080", tls, certFile, keyFile, inmemory)
}

func main() {
	app := cli.NewApp()
	app.Name = "minio"
	app.Usage = "Minio Server"
	var flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "tls",
			Usage: "Enable tls",
		},
		cli.StringFlag{
			Name:  "cert",
			Value: "",
			Usage: "cert file path",
		},
		cli.StringFlag{
			Name:  "key",
			Value: "",
			Usage: "key file path",
		},
		cli.BoolFlag{
			Name:  "inmemory",
			Usage: "in memory storage",
		},
	}
	app.Flags = flags
	app.Action = parseInput
	app.Author = "Minio"
	app.Run(os.Args)
}
