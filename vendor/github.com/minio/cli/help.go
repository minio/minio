package cli

import (
	"fmt"
	"os"
)

// The text template for the Default help topic.
// cli.go uses text/template to render templates. You can
// render custom help text by setting this variable.
var DefaultAppHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.Name}} {{if .Flags}}[global flags] {{end}}command{{if .Flags}} [command flags]{{end}} [arguments...]

COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}{{if .Flags}}
GLOBAL FLAGS:
   {{range .Flags}}{{.}}
   {{end}}{{end}}
VERSION:
   {{.Version}}
{{if .Compiled}}
BUILD:
   {{.Compiled}}{{end}}
{{range $key, $value := ExtraInfo}}
{{$value}}{{end}}
`

// The text template for the command help topic.
// cli.go uses text/template to render templates. You can
// render custom help text by setting this variable.
var DefaultCommandHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   command {{.Name}}{{if .Flags}} [command flags]{{end}} [arguments...]{{if .Description}}

DESCRIPTION:
   {{.Description}}{{end}}{{if .Flags}}

FLAGS:
   {{range .Flags}}{{.}}
   {{end}}{{ end }}
`

// The text template for the subcommand help topic.
// cli.go uses text/template to render templates. You can
// render custom help text by setting this variable.
var DefaultSubcommandHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.Name}} command{{if .Flags}} [command flags]{{end}} [arguments...]

COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}{{if .Flags}}
FLAGS:
   {{range .Flags}}{{.}}
   {{end}}{{end}}
`

var helpCommand = Command{
	Name:    "help",
	Aliases: []string{"h"},
	Usage:   "Shows a list of commands or help for one command",
	Action: func(c *Context) {
		args := c.Args()
		if args.Present() {
			ShowCommandHelp(c, args.First())
		} else {
			ShowAppHelp(c)
		}
	},
	Hide: true,
}

var helpSubcommand = Command{
	Name:    "help",
	Aliases: []string{"h"},
	Usage:   "Shows a list of commands or help for one command",
	Action: func(c *Context) {
		args := c.Args()
		if args.Present() {
			ShowCommandHelp(c, args.First())
		} else {
			ShowSubcommandHelp(c)
		}
	},
	Hide: true,
}

// Prints help for the App
type helpPrinter func(templ string, data interface{})

// HelpPrinter - prints help for the app
var HelpPrinter helpPrinter

// Prints version for the App
var VersionPrinter = printVersion

// ShowAppHelp - Prints the list of subcommands for the app
func ShowAppHelp(c *Context) {
	// Make a copy of c.App context
	app := *c.App
	app.Flags = make([]Flag, 0)
	app.Commands = make([]Command, 0)
	for _, flag := range c.App.Flags {
		if flag.isNotHidden() {
			app.Flags = append(app.Flags, flag)
		}
	}
	for _, command := range c.App.Commands {
		if command.isNotHidden() {
			app.Commands = append(app.Commands, command)
		}
	}
	if app.CustomAppHelpTemplate != "" {
		HelpPrinter(app.CustomAppHelpTemplate, app)
	} else {
		HelpPrinter(DefaultAppHelpTemplate, app)
	}
}

// DefaultAppComplete - Prints the list of subcommands as the default app completion method
func DefaultAppComplete(c *Context) {
	for _, command := range c.App.Commands {
		if command.isNotHidden() {
			for _, name := range command.Names() {
				fmt.Fprintln(c.App.Writer, name)
			}
		}
	}
}

// ShowCommandHelpAndExit - exits with code after showing help
func ShowCommandHelpAndExit(c *Context, command string, code int) {
	ShowCommandHelp(c, command)
	os.Exit(code)
}

// ShowCommandHelp - Prints help for the given command
func ShowCommandHelp(c *Context, command string) {
	// show the subcommand help for a command with subcommands
	if command == "" {
		// Make a copy of c.App context
		app := *c.App
		app.Flags = make([]Flag, 0)
		app.Commands = make([]Command, 0)
		for _, flag := range c.App.Flags {
			if flag.isNotHidden() {
				app.Flags = append(app.Flags, flag)
			}
		}
		for _, command := range c.App.Commands {
			if command.isNotHidden() {
				app.Commands = append(app.Commands, command)
			}
		}
		if app.CustomAppHelpTemplate != "" {
			HelpPrinter(app.CustomAppHelpTemplate, app)
		} else {
			HelpPrinter(DefaultSubcommandHelpTemplate, app)
		}
		return
	}

	for _, c := range c.App.Commands {
		if c.HasName(command) {
			// Make a copy of command context
			c0 := c
			c0.Flags = make([]Flag, 0)
			for _, flag := range c.Flags {
				if flag.isNotHidden() {
					c0.Flags = append(c0.Flags, flag)
				}
			}
			if c0.CustomHelpTemplate != "" {
				HelpPrinter(c0.CustomHelpTemplate, c0)
			} else {
				HelpPrinter(DefaultCommandHelpTemplate, c0)
			}
			return
		}
	}

	if c.App.CommandNotFound != nil {
		c.App.CommandNotFound(c, command)
	} else {
		fmt.Fprintf(c.App.Writer, "No help topic for '%v'\n", command)
	}
}

// ShowSubcommandHelp - Prints help for the given subcommand
func ShowSubcommandHelp(c *Context) {
	ShowCommandHelp(c, c.Command.Name)
}

// ShowVersion - Prints the version number of the App
func ShowVersion(c *Context) {
	VersionPrinter(c)
}

func printVersion(c *Context) {
	fmt.Fprintf(c.App.Writer, "%v version %v\n", c.App.Name, c.App.Version)
}

// ShowCompletions - Prints the lists of commands within a given context
func ShowCompletions(c *Context) {
	a := c.App
	if a != nil && a.BashComplete != nil {
		a.BashComplete(c)
	}
}

// ShowCommandCompletions - Prints the custom completions for a given command
func ShowCommandCompletions(ctx *Context, command string) {
	c := ctx.App.Command(command)
	if c != nil && c.BashComplete != nil {
		c.BashComplete(ctx)
	}
}

func checkVersion(c *Context) bool {
	if c.GlobalBool("version") {
		ShowVersion(c)
		return true
	}

	return false
}

func checkHelp(c *Context) bool {
	if c.GlobalBool("h") || c.GlobalBool("help") {
		ShowAppHelp(c)
		return true
	}

	return false
}

func checkCommandHelp(c *Context, name string) bool {
	if c.Bool("h") || c.Bool("help") {
		ShowCommandHelp(c, name)
		return true
	}

	return false
}

func checkSubcommandHelp(c *Context) bool {
	if c.GlobalBool("h") || c.GlobalBool("help") {
		ShowSubcommandHelp(c)
		return true
	}

	return false
}

func checkCompletions(c *Context) bool {
	if (c.GlobalBool(BashCompletionFlag.Name) || c.Bool(BashCompletionFlag.Name)) && c.App.EnableBashCompletion {
		ShowCompletions(c)
		return true
	}

	return false
}

func checkCommandCompletions(c *Context, name string) bool {
	if c.Bool(BashCompletionFlag.Name) && c.App.EnableBashCompletion {
		ShowCommandCompletions(c, name)
		return true
	}

	return false
}
