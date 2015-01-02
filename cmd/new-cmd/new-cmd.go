package main

import (
	"os"
	"path"
	"text/template"
	"time"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/utils"
)

type source struct {
	Name     string
	TempLate template.Template
}

type option struct {
	Name         string
	Definename   string
	Functionname string
}

type command struct {
	Name    string
	Usage   string
	Month   string
	Year    int
	Options []option
}

func (f source) get(commandName string, definition command) error {
	wr, err := os.Create(path.Join(commandName, f.Name))
	if err != nil {
		return err
	}

	defer wr.Close()
	return f.TempLate.Execute(wr, definition)
}

func initCommand(commandname, usage string, inputOptions []string) command {
	year, month, _ := time.Now().Date()
	return command{
		Name:    commandname,
		Usage:   usage,
		Month:   month.String(),
		Year:    year,
		Options: initOptions(inputOptions),
	}
}

func initOptions(inputOptions []string) []option {
	var options []option

	if inputOptions[0] == "" {
		return options
	}

	for _, name := range inputOptions {
		option := option{
			Name:         name,
			Definename:   utils.FirstUpper(name),
			Functionname: "do" + utils.FirstUpper(name),
		}
		options = append(options, option)
	}

	return options
}

func main() {
	app := cli.NewApp()
	app.Name = "new-cmd"
	app.Usage = "Is a stub builder for new commands, options"
	var flags = []cli.Flag{
		cli.StringFlag{
			Name:  "options",
			Value: "",
			Usage: "Command-separated list of options to build",
		},
		cli.StringFlag{
			Name:  "usage",
			Value: "",
			Usage: "A one liner explaining the new command being built",
		},
	}
	app.Flags = flags
	app.Action = parseInput
	app.Author = "Minio"
	app.Run(os.Args)
}
