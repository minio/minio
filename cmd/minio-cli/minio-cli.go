package main

import (
	"flag"
	"log"
	"os"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/minio-io/minio/pkgs/utils"
)

type source struct {
	Name     string
	TempLate template.Template
}

const (
	// Relative path from GOPATH default
	TEMPLATEREPO = "/src/github.com/minio-io/minio/templates/"
)

type option struct {
	Name         string
	Definename   string
	Functionname string
}

type application struct {
	Name    string
	Usage   string
	Month   string
	Year    int
	Options []option
}

func (f source) generate(appName string, def application) error {
	wr, err := os.Create(path.Join(appName, f.Name))
	if err != nil {
		return err
	}

	defer wr.Close()
	return f.TempLate.Execute(wr, def)
}

func initApplication(appname, usage string, inputOptions []string) application {
	year, month, _ := time.Now().Date()
	return application{
		Name:    appname,
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
	var flOptions, flUsage, flTemplatePath string

	flag.StringVar(&flOptions, "options", "", "Comma-separated list of options to build")
	flag.StringVar(&flUsage, "usage", "", "A one liner explains the purpose of the cli being built")
	flag.StringVar(&flTemplatePath, "templatepath", "", "Non standard templates path")

	flag.Parse()

	inputOptions := strings.Split(flOptions, ",")

	appname := flag.Arg(0)

	if appname == "" {
		log.Fatal("app name must not be blank\n")
	}

	if inputOptions[0] == "" {
		log.Fatal("-options option1 should be specified with appname")
	}

	gopath := os.Getenv("GOPATH")

	var mainTemplatePath, optionsTemplatePath, readmeTemplatePath string
	if flTemplatePath == "" {
		mainTemplatePath = path.Join(gopath, TEMPLATEREPO, "main.tmpl")
		optionsTemplatePath = path.Join(gopath, TEMPLATEREPO, "options.tmpl")
		readmeTemplatePath = path.Join(gopath, TEMPLATEREPO, "README.tmpl")
	} else {
		mainTemplatePath = path.Join(flTemplatePath, "main.tmpl")
		optionsTemplatePath = path.Join(flTemplatePath, "options.tmpl")
		readmeTemplatePath = path.Join(flTemplatePath, "README.tmpl")
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

	if _, err := os.Stat(appname); err == nil {
		// if exists, we overwrite by default
		err = os.RemoveAll(appname)
		utils.Assert(err)
	}

	err := os.Mkdir(appname, 0755)
	utils.Assert(err)

	application := initApplication(appname, flUsage, inputOptions)

	optionsGo := source{
		Name:     appname + "-options.go",
		TempLate: *optionsTemplate,
	}

	readmeMd := source{
		Name:     appname + ".md",
		TempLate: *readmeTemplate,
	}

	mainGo := source{
		Name:     appname + ".go",
		TempLate: *mainTemplate,
	}

	err = readmeMd.generate(appname, application)
	utils.Assert(err)

	mainGo.generate(appname, application)
	utils.Assert(err)

	optionsGo.generate(appname, application)

	err = GoFormat(appname)
	utils.Assert(err)
}
