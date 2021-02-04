/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package console

import (
	"testing"

	"github.com/fatih/color"
)

func TestSetColor(t *testing.T) {
	SetColor("unknown", color.New(color.FgWhite))
	_, ok := Theme["unknown"]
	if !ok {
		t.Fatal("missing theme")
	}
}

func TestColorLock(t *testing.T) {
	Lock()
	Print("") // Test for deadlocks.
	Unlock()
}
