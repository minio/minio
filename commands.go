package main

import (
	"os/user"
	"path"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/server"
)

var commands = []cli.Command{
	modeCmd,
}

var modeCommands = []cli.Command{
	memoryCmd,
	fsCmd,
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

var fsCmd = cli.Command{
	Name:        "fs",
	Description: "Path to filesystem volume.",
	Action:      runFilesystem,
	CustomHelpTemplate: `NAME:
  minio mode {{.Name}} - {{.Description}}

USAGE:
  minio mode {{.Name}} limit SIZE expire TIME

EXAMPLES:
  1. Export an existing filesystem path
      $ minio mode {{.Name}} /var/www

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
  1. Create a donut volume under "/mnt/backup"
      $ minio mode {{.Name}} /mnt/backup

  2. Create a temporary donut volume under "/tmp"
      $ minio mode {{.Name}} /tmp

  3. Create a donut volume under collection of paths
      $ minio mode {{.Name}} /mnt/backup2014feb /mnt/backup2014feb

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
	u, err := user.Current()
	if err != nil {
		Fatalf("Unable to determine current user. Reason: %s\n", err)
	}
	if len(c.Args()) < 1 {
		cli.ShowCommandHelpAndExit(c, "donut", 1) // last argument is exit code
	}
	// supporting multiple paths
	var paths []string
	if strings.TrimSpace(c.Args().First()) == "" {
		p := path.Join(u.HomeDir, "minio-storage", "donut")
		paths = append(paths, p)
	} else {
		for _, arg := range c.Args() {
			paths = append(paths, strings.TrimSpace(arg))
		}
	}
	apiServerConfig := getAPIServerConfig(c)
	donutDriver := server.DonutFactory{
		Config: apiServerConfig,
		Paths:  paths,
	}
	apiServer := donutDriver.GetStartServerFunc()
	//	webServer := getWebServerConfigFunc(c)
	servers := []server.StartServerFunc{apiServer} //, webServer}
	server.StartMinio(servers)
}

func runFilesystem(c *cli.Context) {
	if len(c.Args()) != 1 {
		cli.ShowCommandHelpAndExit(c, "fs", 1) // last argument is exit code
	}
	apiServerConfig := getAPIServerConfig(c)
	fsDriver := server.FilesystemFactory{
		Config: apiServerConfig,
		Path:   c.Args()[0],
	}
	apiServer := fsDriver.GetStartServerFunc()
	//	webServer := getWebServerConfigFunc(c)
	servers := []server.StartServerFunc{apiServer} //, webServer}
	server.StartMinio(servers)
}
