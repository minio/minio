/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package helpers

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
)

// A simple ExecPipe() pipes exec.Cmd together - somewhat similar to how bash pipes "|" behave.
// Each command's standard output is connected to the standard input of the next command
// and the output of the final command is returned

func ExecPipe(cmds ...*exec.Cmd) (pipeLineOutput io.Reader, pipeLineError error) {
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
			return &output, err
		}
	}

	// We should Wait() for each command to complete
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return &output, err
		}
	}

	// Return the output
	return &output, nil
}
