package main

import (
	"os"
	"os/signal"
	"os/user"
	"syscall"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/controller"
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
  1. Get disks from controller
      $ minio {{.Name}} disks http://localhost:9001/rpc

  2. Get memstats from controller
      $ minio {{.Name}} mem http://localhost:9001/rpc

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

func trapServer(doneCh chan struct{}) {
	// Go signal notification works by sending `os.Signal`
	// values on a channel.
	sigs := make(chan os.Signal, 1)

	// `signal.Notify` registers the given channel to
	// receive notifications of the specified signals.
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGUSR2)

	// This executes a blocking receive for signals.
	// When it gets one it'll then notify the program
	// that it can finish.
	<-sigs
	doneCh <- struct{}{}
}

func runServer(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	doneCh := make(chan struct{})
	go trapServer(doneCh)

	apiServerConfig := getServerConfig(c)
	err = server.StartServices(apiServerConfig, doneCh)
	if err != nil {
		Fatalln(err)
	}
}

func runController(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	if len(c.Args()) <= 2 || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "controller", 1) // last argument is exit code
	}
	switch c.Args().First() {
	case "disks":
		disks, err := controller.GetDisks(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(disks)
	case "mem":
		memstats, err := controller.GetMemStats(c.Args().Tail().First())
		if err != nil {
			Fatalln(err)
		}
		Println(string(memstats))
	case "donut":
		hostname, _ := os.Hostname()
		err := controller.SetDonut(c.Args().Tail().First(), hostname, c.Args().Tail().Tail())
		if err != nil {
			Fatalln(err)
		}
	}
}
