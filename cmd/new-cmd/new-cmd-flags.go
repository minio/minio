package main

import (
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/utils"
)

func parseInput(c *cli.Context) {
	var commandName string
	switch len(c.Args()) {
	case 1:
		commandName = c.Args()[0]
	default:
		log.Fatal("command name must not be blank\n")
	}

	var inputOptions []string
	if c.String("options") != "" {
		inputOptions = strings.Split(c.String("options"), ",")
	}

	if inputOptions[0] == "" {
		log.Fatal("options cannot be empty with a command name")
	}

	var commandUsage string
	if c.String("usage") != "" {
		commandUsage = c.String("usage")
	}

	var templatePath string
	if c.String("path") != "" {
		templatePath = c.String("path")
	}

	gopath := os.Getenv("GOPATH")

	var mainTemplatePath, optionsTemplatePath, readmeTemplatePath string
	if templatePath == TEMPLATEREPO {
		mainTemplatePath = path.Join(gopath, templatePath, "main.tmpl")
		optionsTemplatePath = path.Join(gopath, templatePath, "options.tmpl")
		readmeTemplatePath = path.Join(gopath, templatePath, "README.tmpl")
	} else {
		mainTemplatePath = path.Join(templatePath, "main.tmpl")
		optionsTemplatePath = path.Join(templatePath, "options.tmpl")
		readmeTemplatePath = path.Join(templatePath, "README.tmpl")
	}
	if _, err := os.Stat(mainTemplatePath); err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(optionsTemplatePath); err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(readmeTemplatePath); err != nil {
		log.Fatal(err)
	}

	var mainTemplate = template.Must(template.ParseFiles(mainTemplatePath))
	var optionsTemplate = template.Must(template.ParseFiles(optionsTemplatePath))
	var readmeTemplate = template.Must(template.ParseFiles(readmeTemplatePath))

	err := os.Mkdir(commandName, 0755)
	utils.Assert(err)

	command := initCommand(commandName, commandUsage, inputOptions)

	optionsGo := source{
		Name:     commandName + "-options.go",
		TempLate: *optionsTemplate,
	}

	readmeMd := source{
		Name:     commandName + ".md",
		TempLate: *readmeTemplate,
	}

	mainGo := source{
		Name:     commandName + ".go",
		TempLate: *mainTemplate,
	}

	err = readmeMd.get(commandName, command)
	utils.Assert(err)

	mainGo.get(commandName, command)
	utils.Assert(err)

	optionsGo.get(commandName, command)

	err = GoFormat(commandName)
	utils.Assert(err)
}
