package main

import (
	"os/user"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/server"
	"github.com/minio/minio/pkg/server/api"
)

var commands = []cli.Command{
	serverCmd,
	controllerCmd,
}

var serverCmd = cli.Command{
	Name:        "server",
	Description: "Server mode",
	Action:      runServer,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Start in server mode
      $ minio {{.Name}}

`,
}

var controllerCmd = cli.Command{
	Name:        "controller",
	Description: "Control mode",
	Action:      runController,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Start in controller mode
      $ minio {{.Name}}

`,
}

func getServerConfig(c *cli.Context) api.Config {
	certFile := c.GlobalString("cert")
	keyFile := c.GlobalString("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		Fatalln("Both certificate and key are required to enable https.")
	}
	tls := (certFile != "" && keyFile != "")
	return api.Config{
		Address:   c.GlobalString("address"),
		TLS:       tls,
		CertFile:  certFile,
		KeyFile:   keyFile,
		RateLimit: c.GlobalInt("ratelimit"),
	}
}

func runServer(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	apiServerConfig := getServerConfig(c)
	if err := server.StartServices(apiServerConfig); err != nil {
		Fatalln(err)
	}
}

func runController(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
}
