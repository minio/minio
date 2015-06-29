package main

import (
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/server"
)

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

var commands = []cli.Command{
	modeCmd,
}

var modeCommands = []cli.Command{
	memoryCmd,
	donutCmd,
}

var modeCmd = cli.Command{
	Name:        "mode",
	Subcommands: modeCommands,
	Description: "Mode of execution",
}

var memoryCmd = cli.Command{
	Name:        "memory",
	Description: "Limit maximum memory usage to SIZE in [B, KB, MB, GB]",
	Action:      runMemory,
	CustomHelpTemplate: `NAME:
  minio mode {{.Name}} - {{.Description}}

USAGE:
  minio mode {{.Name}} limit SIZE expire TIME

EXAMPLES:
  1. Limit maximum memory usage to 64MB with 1 hour expiration
      $ minio mode {{.Name}} limit 64MB expire 1h

  2. Limit maximum memory usage to 4GB with no expiration
      $ minio mode {{.Name}} limit 4GB
`,
}

var donutCmd = cli.Command{
	Name:        "donut",
	Description: "[status: EXPERIMENTAL]. Path to donut volume.",
	Action:      runDonut,
	CustomHelpTemplate: `NAME:
  minio mode {{.Name}} - {{.Description}}

USAGE:
  minio mode {{.Name}} PATH

EXAMPLES:
  1. Create a donut volume under "/mnt/backup", with a cache limit of 64MB with 1hr expiration
      $ minio mode {{.Name}} limit 64MB expire 1h paths /mnt/backup

  2. Create a donut volume under collection of paths, put a cache limit of 512MB
      $ minio mode {{.Name}} limit 512MB paths ""

`,
}

func runMemory(c *cli.Context) {
	if len(c.Args()) == 0 || len(c.Args())%2 != 0 {
		cli.ShowCommandHelpAndExit(c, "memory", 1) // last argument is exit code
	}
	apiServerConfig := getAPIServerConfig(c)

	var maxMemory uint64
	maxMemorySet := false

	var expiration time.Duration
	expirationSet := false

	var err error

	args := c.Args()
	for len(args) > 0 {
		switch args.First() {
		case "limit":
			{
				if maxMemorySet {
					Fatalln("Limit should be set only once")
				}
				args = args.Tail()
				maxMemory, err = humanize.ParseBytes(args.First())
				if err != nil {
					Fatalf("Invalid memory size [%s] passed. Reason: %s\n", args.First(), iodine.New(err, nil))
				}
				if maxMemory < 1024*1024*10 {
					Fatalf("Invalid memory size [%s] passed. Should be greater than 10M\n", args.First())
				}
				args = args.Tail()
				maxMemorySet = true
			}
		case "expire":
			{
				if expirationSet {
					Fatalln("Expiration should be set only once")
				}
				args = args.Tail()
				expiration, err = time.ParseDuration(args.First())
				if err != nil {
					Fatalf("Invalid expiration time [%s] passed. Reason: %s\n", args.First(), iodine.New(err, nil))
				}
				args = args.Tail()
				expirationSet = true
			}
		default:
			{
				cli.ShowCommandHelpAndExit(c, "memory", 1) // last argument is exit code
			}
		}
	}
	if maxMemorySet == false {
		Fatalln("Memory limit must be set")
	}
	memoryDriver := server.MemoryFactory{
		Config:     apiServerConfig,
		MaxMemory:  maxMemory,
		Expiration: expiration,
	}
	apiServer := memoryDriver.GetStartServerFunc()
	//	webServer := getWebServerConfigFunc(c)
	servers := []server.StartServerFunc{apiServer} //, webServer}
	server.StartMinio(servers)
}

func runDonut(c *cli.Context) {
	var err error

	u, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	if len(c.Args()) < 1 {
		cli.ShowCommandHelpAndExit(c, "donut", 1) // last argument is exit code
	}
	var maxMemory uint64
	maxMemorySet := false

	var expiration time.Duration
	expirationSet := false

	var paths []string
	pathSet := false

	args := c.Args()
	for len(args) > 0 {
		switch args.First() {
		case "limit":
			{
				if maxMemorySet {
					Fatalln("Limit should be set only once")
				}
				args = args.Tail()
				maxMemory, err = humanize.ParseBytes(args.First())
				if err != nil {
					Fatalf("Invalid memory size [%s] passed. Reason: %s\n", args.First(), iodine.New(err, nil))
				}
				if maxMemory < 1024*1024*10 {
					Fatalf("Invalid memory size [%s] passed. Should be greater than 10M\n", args.First())
				}
				args = args.Tail()
				maxMemorySet = true
			}
		case "expire":
			{
				if expirationSet {
					Fatalln("Expiration should be set only once")
				}
				args = args.Tail()
				expiration, err = time.ParseDuration(args.First())
				if err != nil {
					Fatalf("Invalid expiration time [%s] passed. Reason: %s\n", args.First(), iodine.New(err, nil))
				}
				args = args.Tail()
				expirationSet = true
			}
		case "paths":
			if pathSet {
				Fatalln("Path should be set only once")
			}
			// supporting multiple paths
			args = args.Tail()
			if strings.TrimSpace(args.First()) == "" {
				p := filepath.Join(u.HomeDir, "minio-storage", "donut")
				paths = appendUniq(paths, p)
			} else {
				for _, arg := range args {
					paths = appendUniq(paths, strings.TrimSpace(arg))
				}
			}
			args = args.Tail()
			pathSet = true
		default:
			{
				cli.ShowCommandHelpAndExit(c, "donut", 1) // last argument is exit code
			}
		}
	}
	if maxMemorySet == false {
		Fatalln("Memory limit must be set")
	}
	if pathSet == false {
		Fatalln("Path must be set")
	}
	apiServerConfig := getAPIServerConfig(c)
	donutDriver := server.DonutFactory{
		Config:     apiServerConfig,
		Paths:      paths,
		MaxMemory:  maxMemory,
		Expiration: expiration,
	}
	apiServer := donutDriver.GetStartServerFunc()
	//	webServer := getWebServerConfigFunc(c)
	servers := []server.StartServerFunc{apiServer} //, webServer}
	server.StartMinio(servers)
}
