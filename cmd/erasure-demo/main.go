package main

import (
	"os"

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
