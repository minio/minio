/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package os

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMkdir(t *testing.T) {
	tempDir, err := ioutil.TempDir("", ".TestMkdir.")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	defer os.RemoveAll(tempDir)

	case1Name := filepath.Join(tempDir, "name1")

	case2Name := filepath.Join(tempDir, "name2")
	if err = ioutil.WriteFile(case2Name, []byte{}, os.ModePerm); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		name      string
		expectErr bool
	}{
		{case1Name, false},
		{case1Name, false}, // no error on already existing directory.
		{case2Name, true},
	}

	if runtime.GOOS != "windows" {
		case3Name := filepath.Join(tempDir, "name3")
		if err = os.Mkdir(case3Name, os.ModePerm); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err = os.Chmod(case3Name, 0755); err != nil {
			t.Fatalf("unexpected error %v", err)
		}

		case4Name := filepath.Join(tempDir, "name4")
		if err = os.Mkdir(case4Name, os.ModePerm); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err = os.Chmod(case4Name, 0575); err != nil {
			t.Fatalf("unexpected error %v", err)
		}

		case5Name := filepath.Join(tempDir, "name5")
		if err = os.Mkdir(case5Name, os.ModePerm); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if err = os.Chmod(case5Name, 0557); err != nil {
			t.Fatalf("unexpected error %v", err)
		}

		testCases = append(testCases,
			struct {
				name      string
				expectErr bool
			}{case3Name, false},
			struct {
				name      string
				expectErr bool
			}{case4Name, true},
			struct {
				name      string
				expectErr bool
			}{case5Name, true},
		)
	}

	for i, testCase := range testCases {
		err := Mkdir(testCase.name, os.ModePerm)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}
