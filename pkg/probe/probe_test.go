/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
package probe

import (
	"os"
	"testing"
)

func testDummy() *Error {
	_, e := os.Stat("this-file-cannot-exit-234234232423423")
	es := New(e)
	es.Trace("this is hell", "asdf")
	return es
}

func TestProbe(t *testing.T) {
	es := testDummy()
	if es == nil {
		t.Fail()
	}

	newES := es.Trace()
	if newES == nil {
		t.Fail()
	}
	/*
		fmt.Println(es)
		fmt.Println(es.JSON())
		fmt.Println(es.ToError())
	*/
}
