package main

import (
	"fmt"
	"github.com/GeertJohan/go.linenoise"
	"os"
	"strings"
)

func main() {
	fmt.Println("Welcome to go.linenoise example.")
	writeHelp()
	for {
		str, err := linenoise.Line("prompt> ")
		if err != nil {
			if err == linenoise.KillSignalError {
				quit()
			}
			fmt.Println("Unexpected error: %s", err)
			quit()
		}
		fields := strings.Fields(str)

		// check if there is any valid input at all
		if len(fields) == 0 {
			writeUnrecognized()
			continue
		}

		// switch on the command
		switch fields[0] {
		case "help":
			writeHelp()
		case "echo":
			fmt.Printf("echo: %s\n\n", str[5:])
		case "clear":
			linenoise.Clear()
		case "multiline":
			fmt.Println("Setting linenoise to multiline")
			linenoise.SetMultiline(true)
		case "singleline":
			fmt.Println("Setting linenoise to singleline")
			linenoise.SetMultiline(false)
		case "complete":
			fmt.Println("Setting arguments as completion values for linenoise.")
			fmt.Printf("%d arguments: %s\n", len(fields)-1, fields[1:])
			completionHandler := func(in string) []string {
				return fields[1:]
			}
			linenoise.SetCompletionHandler(completionHandler)
		case "printKeyCodes":
			linenoise.PrintKeyCodes()
		case "addHistory":
			if len(str) < 12 {
				fmt.Println("No argument given.")
			}
			err := linenoise.AddHistory(str[11:])
			if err != nil {
				fmt.Printf("Error: %s\n", err)
			}
		case "save":
			if len(fields) != 2 {
				fmt.Println("Error. Expecting 'save <filename>'.")
				continue
			}
			err := linenoise.SaveHistory(fields[1])
			if err != nil {
				fmt.Printf("Error on save: %s\n", err)
			}
		case "load":
			if len(fields) != 2 {
				fmt.Println("Error. Expecting 'load <filename>'.")
				continue
			}
			err := linenoise.LoadHistory(fields[1])
			if err != nil {
				fmt.Printf("Error on load: %s\n", err)
			}
		case "quit":
			quit()
		default:
			writeUnrecognized()
		}
	}
}

func quit() {
	fmt.Println("Thanks for running the go.linenoise example.")
	fmt.Println("")
	os.Exit(0)
}

func writeHelp() {
	fmt.Println("help              write this message")
	fmt.Println("echo ...          echo the arguments")
	fmt.Println("clear             clear the screen")
	fmt.Println("multiline         set linenoise to multiline")
	fmt.Println("singleline        set linenoise to singleline")
	fmt.Println("complete ...      set arguments as completion values")
	fmt.Println("addHistory ...    add arguments to linenoise history")
	fmt.Println("save <filename>   save the history to file")
	fmt.Println("load <filename>   load the history from file")
	fmt.Println("quit              stop the program")
	fmt.Println("")
	fmt.Println("Use the arrow up and down keys to walk through history.")
	fmt.Println("Note that you have to use addHistory to create history entries. Commands are not added to history in this example.")
	fmt.Println("")
}

func writeUnrecognized() {
	fmt.Println("Unrecognized command. Use 'help'.")
}
