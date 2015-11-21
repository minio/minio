package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"io/ioutil"
	"text/tabwriter"
	"text/template"
)

// App is the main structure of a cli application. It is recomended that
// and app be created with the cli.NewApp() function
type App struct {
	// The name of the program. Defaults to os.Args[0]
	Name string
	// Usage of the program.
	Usage string
	// Description of the program.
	Description string
	// Version of the program
	Version string
	// List of commands to execute
	Commands []Command
	// List of flags to parse
	Flags []Flag
	// Boolean to enable bash completion commands
	EnableBashCompletion bool
	// An action to execute when the bash-completion flag is set
	BashComplete func(context *Context)
	// An action to execute before any subcommands are run, but after the context is ready
	// If a non-nil error is returned, no subcommands are run
	Before func(context *Context) error
	// An action to execute after any subcommands are run, but after the subcommand has finished
	// It is run even if Action() panics
	After func(context *Context) error
	// The action to execute when no subcommands are specified
	Action func(context *Context)
	// Execute this function if the proper command cannot be found
	CommandNotFound func(context *Context, command string)
	// Compilation date
	Compiled string
	// ExtraInfo pass additional info as a key value map
	ExtraInfo func() map[string]string
	// List of all authors who contributed
	Authors []Author
	// Name of Author (Note: Use App.Authors, this is deprecated)
	Author string
	// Email of Author (Note: Use App.Authors, this is deprecated)
	Email string
	// Writer writer to write output to
	Writer io.Writer
	// CustomAppHelpTemplate the text template for app help topic.
	// cli.go uses text/template to render templates. You can
	// render custom help text by setting this variable.
	CustomAppHelpTemplate string
}

// mustCompileTime - determines the modification time of the current binary
func mustCompileTime() string {
	path, err := exec.LookPath(os.Args[0])
	if err != nil {
		return ""
	}

	info, err := os.Stat(path)
	if err != nil {
		return ""
	}
	return info.ModTime().String()
}

// NewApp - Creates a new cli Application with some reasonable defaults for Name, Usage, Version and Action.
func NewApp() *App {
	return &App{
		Name:         os.Args[0],
		Usage:        "A new cli application",
		Version:      "0.0.0",
		Action:       helpCommand.Action,
		BashComplete: DefaultAppComplete,
		Compiled:     mustCompileTime(),
		Writer:       os.Stdout,
	}
}

// getNewContext -
func (a *App) getNewContext(arguments []string) (*Context, error) {
	// parse flags
	set := flagSet(a.Name, a.Flags)
	set.SetOutput(ioutil.Discard)
	context := NewContext(a, set, set)

	err := set.Parse(arguments[1:])
	if err != nil {
		if len(arguments[1:]) > 1 {
			fmt.Fprint(a.Writer, fmt.Sprintf("Unknown flags. ‘%s’\n\n", strings.Join(arguments[1:], ", ")))
		} else {
			fmt.Fprint(a.Writer, fmt.Sprintf("Unknown flag. ‘%s’\n\n", strings.Join(arguments[1:], ", ")))
		}
		ShowAppHelp(context)
		fmt.Fprint(a.Writer, "")
		return nil, err

	}
	nerr := normalizeFlags(a.Flags, set)
	if nerr != nil {
		fmt.Fprintln(a.Writer, nerr)
		ShowAppHelp(context)
		fmt.Fprint(a.Writer, "")
		return nil, nerr
	}
	return context, nil
}

// Run - Entry point to the cli app. Parses the arguments slice and routes to the proper flag/args combination
func (a *App) Run(arguments []string) (err error) {
	if HelpPrinter == nil {
		defer func() {
			HelpPrinter = nil
		}()

		HelpPrinter = func(templ string, data interface{}) {
			funcMap := template.FuncMap{}
			funcMap["join"] = strings.Join
			// if ExtraInfo function
			funcMap["ExtraInfo"] = func() map[string]string { return make(map[string]string) }
			if a.ExtraInfo != nil {
				funcMap["ExtraInfo"] = a.ExtraInfo
			}
			w := tabwriter.NewWriter(a.Writer, 0, 8, 1, '\t', 0)
			t := template.Must(template.New("help").Funcs(funcMap).Parse(templ))
			err := t.Execute(w, data)
			switch e := err.(type) {
			case *os.PathError:
				if e.Err == syscall.EPIPE {
					break
				}
				if err != nil {
					panic(err)
				}
			}
			w.Flush()
		}
	}

	// append version/help flags
	if a.EnableBashCompletion {
		a.appendFlag(BashCompletionFlag)
	}

	context, err := a.getNewContext(arguments)
	if err != nil {
		return err
	}

	if checkCompletions(context) || checkHelp(context) || checkVersion(context) {
		return nil
	}

	if a.After != nil {
		defer func() {
			// err is always nil here.
			// There is a check to see if it is non-nil
			// just few lines before.
			err = a.After(context)
		}()
	}

	if a.Before != nil {
		err := a.Before(context)
		if err != nil {
			return err
		}
	}

	args := context.Args()
	if args.Present() {
		name := args.First()
		c := a.Command(name)
		if c != nil {
			return c.Run(context)
		}
	}

	// Run default Action
	a.Action(context)
	return nil
}

// RunAndExitOnError - Another entry point to the cli app, takes care of passing arguments and error handling
func (a *App) RunAndExitOnError() {
	if err := a.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

// RunAsSubcommand - Invokes the subcommand given the context, parses ctx.Args() to generate command-specific flags
func (a *App) RunAsSubcommand(ctx *Context) (err error) {
	// append flags
	if a.EnableBashCompletion {
		a.appendFlag(BashCompletionFlag)
	}

	// parse flags
	set := flagSet(a.Name, a.Flags)
	set.SetOutput(ioutil.Discard)
	err = set.Parse(ctx.Args().Tail())
	nerr := normalizeFlags(a.Flags, set)
	context := NewContext(a, set, ctx.globalSet)

	if nerr != nil {
		fmt.Fprintln(a.Writer, nerr)
		if len(a.Commands) > 0 {
			ShowSubcommandHelp(context)
		} else {
			ShowCommandHelp(ctx, context.Args().First())
		}
		fmt.Fprint(a.Writer, "")
		return nerr
	}

	if err != nil {
		if len(ctx.Args().Tail()) > 1 {
			fmt.Fprint(a.Writer, fmt.Sprintf("Unknown flags. ‘%s’\n\n", strings.Join(ctx.Args().Tail(), ", ")))
		} else {
			fmt.Fprint(a.Writer, fmt.Sprintf("Unknown flag. ‘%s’\n\n", ctx.Args().Tail()[0]))
		}
		ShowSubcommandHelp(context)
		return err
	}

	if checkCompletions(context) {
		return nil
	}

	if len(a.Commands) > 0 {
		if checkSubcommandHelp(context) {
			return nil
		}
	} else {
		if checkCommandHelp(ctx, context.Args().First()) {
			return nil
		}
	}

	if a.After != nil {
		defer func() {
			// err is always nil here.
			// There is a check to see if it is non-nil
			// just few lines before.
			err = a.After(context)
		}()
	}

	if a.Before != nil {
		err := a.Before(context)
		if err != nil {
			return err
		}
	}

	args := context.Args()
	if args.Present() {
		name := args.First()
		c := a.Command(name)
		if c != nil {
			return c.Run(context)
		}
		fmt.Fprint(ctx.App.Writer, fmt.Sprintf("Unknown flag. ‘%s’\n\n", name))
		ShowSubcommandHelp(context)
		return errors.New("Command not found")
	}

	// Run default Action
	a.Action(context)
	return nil
}

// Command - Returns the named command on App. Returns nil if the command does not exist
func (a *App) Command(name string) *Command {
	for _, c := range a.Commands {
		if c.HasName(name) {
			return &c
		}
	}

	return nil
}

func (a *App) hasFlag(flag Flag) bool {
	for _, f := range a.Flags {
		if flag == f {
			return true
		}
	}

	return false
}

func (a *App) appendFlag(flag Flag) {
	if !a.hasFlag(flag) {
		a.Flags = append(a.Flags, flag)
	}
}

// Author represents someone who has contributed to a cli project.
type Author struct {
	Name  string // The Authors name
	Email string // The Authors email
}

// String makes Author comply to the Stringer interface, to allow an easy print in the templating process
func (a Author) String() string {
	e := ""
	if a.Email != "" {
		e = "<" + a.Email + "> "
	}

	return fmt.Sprintf("%v %v", a.Name, e)
}
