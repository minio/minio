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

package cmd

import (
	"testing"
	"time"
)

func TestGoroutineCountCheck(t *testing.T) {
	tests := []struct {
		threshold int
		wantErr   bool
	}{
		{5000, false},
		{5, true},
		{6, true},
	}
	for _, tt := range tests {
		// Make goroutines -- to make sure number of go-routines is higher than threshold
		if tt.threshold == 5 || tt.threshold == 6 {
			for i := 0; i < 6; i++ {
				go time.Sleep(5)
			}
		}
		if err := goroutineCountCheck(tt.threshold); (err != nil) != tt.wantErr {
			t.Errorf("goroutineCountCheck() error = %v, wantErr %v", err, tt.wantErr)
		}
	}
}
