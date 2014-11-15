package main

import (
	"github.com/codegangsta/cli"
	"log"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Commands = []cli.Command{
		{
			Name:  "put",
			Usage: "Start a storage node",
			Action: func(c *cli.Context) {
			},
		},
		{
			Name:  "get",
			Usage: "Start a gateway node",
			Action: func(c *cli.Context) {
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal("App failed to load", err)
	}
}
