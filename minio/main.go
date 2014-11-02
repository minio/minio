package main

import (
	"github.com/codegangsta/cli"
	"github.com/minios/minios"
	"os"
)

func main() {
	cli.NewApp().Run(os.Args)
	server := minio.Server{}
	server.Start()
}
