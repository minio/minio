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

package erasure

import (
	"bytes"
	. "gopkg.in/check.v1"
	"testing"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestCauchyDecode(c *C) {
	ep, _ := ParseEncoderParams(10, 5, CAUCHY)

	data := []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.")

	e := NewEncoder(ep)
	chunks, length := e.Encode(data)
	c.Assert(length, Equals, len(data))

	chunks[0] = nil
	chunks[3] = nil
	chunks[5] = nil
	chunks[9] = nil
	chunks[13] = nil

	recovered_data, err := e.Decode(chunks, length)
	c.Assert(err, IsNil)

	if !bytes.Equal(data, recovered_data) {
		c.Fatalf("Recovered data mismatches with original data")
	}
}
