package cli

import (
	"fmt"
	"io/ioutil"
	"strings"
)

// Command is a subcommand for a cli.App.
type Command struct {
	// The name of the command
	Name string
	// short name of the command. Typically one character (deprecated, use `Aliases`)
	ShortName string
	// A list of aliases for the command
	Aliases []string
	// A short description of the usage of this command
	Usage string
	// A longer explanation of how the command works
	Description string
	// The function to call when checking for bash command completions
	BashComplete func(context *Context)
	// An action to execute before any sub-subcommands are run, but after the context is ready
	// If a non-nil error is returned, no sub-subcommands are run
	Before func(context *Context) error
	// An action to execute after any subcommands are run, but after the subcommand has finished
	// It is run even if Action() panics
	After func(context *Context) error
	// The function to call when this command is invoked
	Action func(context *Context)
	// List of child commands
	Subcommands []Command
	// List of flags to parse
	Flags []Flag
	// Treat all flags as normal arguments if true
	SkipFlagParsing bool
	// Boolean to hide this command from help or completion
	Hide bool
	// CustomHelpTemplate the text template for the command help topic.
	// cli.go uses text/template to render templates. You can
	// render custom help text by setting this variable.
	CustomHelpTemplate string
}

// Run - Invokes the command given the context, parses ctx.Args() to generate command-specific flags
func (c Command) Run(ctx *Context) error {
	if len(c.Subcommands) > 0 || c.Before != nil || c.After != nil {
		return c.startApp(ctx)
	}

	if ctx.App.EnableBashCompletion {
		c.Flags = append(c.Flags, BashCompletionFlag)
	}

	set := flagSet(c.Name, c.Flags)
	set.SetOutput(ioutil.Discard)

	firstFlagIndex := -1
	terminatorIndex := -1
	for index, arg := range ctx.Args() {
		if arg == "--" {
			terminatorIndex = index
			break
		} else if strings.HasPrefix(arg, "-") && firstFlagIndex == -1 {
			firstFlagIndex = index
		}
	}

	var err error
	if firstFlagIndex > -1 && !c.SkipFlagParsing {
		args := ctx.Args()
		regularArgs := make([]string, len(args[1:firstFlagIndex]))
		copy(regularArgs, args[1:firstFlagIndex])

		var flagArgs []string
		if terminatorIndex > -1 {
			flagArgs = args[firstFlagIndex:terminatorIndex]
			regularArgs = append(regularArgs, args[terminatorIndex:]...)
		} else {
			flagArgs = args[firstFlagIndex:]
		}

		err = set.Parse(append(flagArgs, regularArgs...))
	} else {
		err = set.Parse(ctx.Args().Tail())
	}

	if err != nil {
		if len(ctx.Args().Tail()) > 1 {
			fmt.Fprint(ctx.App.Writer, fmt.Sprintf("Unknown flags. ‘%s’\n\n", strings.Join(ctx.Args().Tail(), ", ")))
		} else {
			fmt.Fprint(ctx.App.Writer, fmt.Sprintf("Unknown flag. ‘%s’\n\n", ctx.Args().Tail()[0]))
		}
		ShowCommandHelp(ctx, c.Name)
		fmt.Fprint(ctx.App.Writer, "")
		return err
	}

	nerr := normalizeFlags(c.Flags, set)
	if nerr != nil {
		fmt.Fprintln(ctx.App.Writer, nerr)
		fmt.Fprintln(ctx.App.Writer)
		ShowCommandHelp(ctx, c.Name)
		fmt.Fprint(ctx.App.Writer, "")
		return nerr
	}
	context := NewContext(ctx.App, set, ctx.globalSet)

	if checkCommandCompletions(context, c.Name) {
		return nil
	}

	if checkCommandHelp(context, c.Name) {
		return nil
	}
	context.Command = c
	c.Action(context)
	return nil
}

// Names - returns collection of all name, shortname and aliases
func (c Command) Names() []string {
	names := []string{c.Name}

	if c.ShortName != "" {
		names = append(names, c.ShortName)
	}

	return append(names, c.Aliases...)
}

// HasName - Returns true if Command.Name or Command.ShortName matches given name
func (c Command) HasName(name string) bool {
	for _, n := range c.Names() {
		if n == name {
			return true
		}
	}
	return false
}

func (c Command) isNotHidden() bool {
	return !c.Hide
}

func (c Command) startApp(ctx *Context) error {
	app := NewApp()

	// set the name and usage
	app.Name = fmt.Sprintf("%s %s", ctx.App.Name, c.Name)
	if c.Description != "" {
		app.Usage = c.Description
	} else {
		app.Usage = c.Usage
	}

	// set CommandNotFound
	app.CommandNotFound = ctx.App.CommandNotFound
	app.CustomAppHelpTemplate = c.CustomHelpTemplate

	// set the flags and commands
	app.Commands = c.Subcommands
	app.Flags = c.Flags

	// bash completion
	app.EnableBashCompletion = ctx.App.EnableBashCompletion
	if c.BashComplete != nil {
		app.BashComplete = c.BashComplete
	}

	// set the actions
	app.Before = c.Before
	app.After = c.After
	if c.Action != nil {
		app.Action = c.Action
	} else {
		app.Action = helpSubcommand.Action
	}
	return app.RunAsSubcommand(ctx)
}
