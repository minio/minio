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

package sia

import (
	"testing"
)

func TestSianon2xx(t *testing.T) {
	for i := 0; i < 1000; i++ {
		actual := non2xx(i)
		expected := i < 200 || i > 299

		if actual != expected {
			t.Errorf("Test case %d: non2xx(%d) returned %t but expected %t", i+1, i, actual, expected)
		}
	}
}
