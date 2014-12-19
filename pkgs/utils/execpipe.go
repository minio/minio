package utils

import (
	"bytes"
	"errors"
	"os/exec"
)

// A simple ExecPipe() pipes exec.Cmd together - somewhat similar to how bash pipes "|" behave.
// Each command's standard output is connected to the standard input of the next command
// and the output of the final command is returned

func ExecPipe(cmds ...*exec.Cmd) (pipeLineOutput []byte, pipeLineError error) {
	// Require at least one command
	if len(cmds) < 1 {
		return nil, errors.New("Invalid argument")
	}

	// Collect the output from the command(s)
	var output bytes.Buffer

	lastIndex := len(cmds) - 1
	for i, cmd := range cmds[:lastIndex] {
		cmds[i+1].Stdin, _ = cmd.StdoutPipe()
	}

	// Final ---> output buffer
	cmds[lastIndex].Stdout = &output

	// Start each command
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return output.Bytes(), err
		}
	}

	// We should Wait() for each command to complete
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return output.Bytes(), err
		}
	}

	// Return the output
	return output.Bytes(), nil
}
