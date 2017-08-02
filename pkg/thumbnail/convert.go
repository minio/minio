/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package thumbnail

import (
	"io"
	"os/exec"
)

// Attempts to resize an image using the `convert` command line tool.
func resizeUsingConvert(input io.Reader, output io.Writer, dimensions, format string) error {
	// Call into convert.
	cmd := exec.Command("convert", "-", "-thumbnail", dimensions, format+":-")
	cmd.Stdin = input
	cmd.Stdout = output

	err := cmd.Run()
	return err
}
