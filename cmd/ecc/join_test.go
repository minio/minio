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

package ecc

import (
	"errors"
	"fmt"
	"testing"
)

var CountErrorTests = []struct {
	Err []error
	N   int
}{
	{Err: make([]error, 4), N: 0},                                             // 0
	{Err: make([]error, 16), N: 0},                                            // 1
	{Err: []error{errOffline, errOffline, nil, nil}, N: 2},                    // 2
	{Err: []error{errOffline, errOffline, errOffline, errOffline}, N: 4},      // 3
	{Err: []error{errOffline, errOffline, errors.New("an error"), nil}, N: 3}, // 4
}

func TestCountErrors(t *testing.T) {
	for i := range CountErrorTests {
		t.Run(fmt.Sprintf("Test %d", i), func(t *testing.T) {
			test := CountErrorTests[i]

			if n := countErrors(test.Err); n != test.N {
				t.Errorf("got %d - want %d", n, test.N)
			}
		})
	}
}
