package main

import (
	"os/user"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/server"
)

func removeDuplicates(slice []string) []string {
	newSlice := []string{}
	seen := make(map[string]struct{})
	for _, val := range slice {
		if _, ok := seen[val]; !ok {
			newSlice = append(newSlice, val)
			seen[val] = struct{}{}
		}
	}
	return newSlice
}

var commands = []cli.Command{
	serverCmd,
	controlCmd,
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
      $ minio server

`,
}

var controlCmd = cli.Command{
	Name:        "control",
	Description: "Control mode",
	Action:      runController,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Description}}

USAGE:
  minio {{.Name}}

EXAMPLES:
  1. Start in controller mode
      $ minio control

`,
}

func runServer(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	if len(c.Args()) < 1 {
		cli.ShowCommandHelpAndExit(c, "server", 1) // last argument is exit code
	}
	apiServerConfig := getAPIServerConfig(c)
	s := server.Factory{
		Config: apiServerConfig,
	}
	apiServer := s.GetStartServerFunc()
	//	webServer := getWebServerConfigFunc(c)
	servers := []server.StartServerFunc{apiServer} //, webServer}
	server.StartMinio(servers)
}

func runController(c *cli.Context) {
	_, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	if len(c.Args()) < 1 {
		cli.ShowCommandHelpAndExit(c, "control", 1) // last argument is exit code
	}
	apiServerConfig := getAPIServerConfig(c)
	s := server.Factory{
		Config: apiServerConfig,
	}
	apiServer := s.GetStartServerFunc()
	//	webServer := getWebServerConfigFunc(c)
	servers := []server.StartServerFunc{apiServer} //, webServer}
	server.StartMinio(servers)
}
