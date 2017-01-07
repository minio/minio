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

package cmd

import (
	"testing"

	"github.com/minio/cli"
)

// Tests register command function.
func TestRegisterCommand(t *testing.T) {
	registerCommand(cli.Command{
		Name: "test1",
	})
	ccount := len(commands)
	if ccount != 1 {
		t.Fatalf("Unexpected number of commands found %d", ccount)
	}
	registerCommand(cli.Command{
		Name: "test2",
	})
	ccount = len(commands)
	if ccount != 2 {
		t.Fatalf("Unexpected number of commands found %d", ccount)
	}
}
