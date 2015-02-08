/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package tiedot

import (
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) Testing(c *C) {
	d, err := NewDatabase("/tmp/testdata")
	defer os.RemoveAll("/tmp/testdata")
	c.Assert(err, IsNil)

	d.InitCollection("Matrix")

	data := map[string]interface{}{
		"version":  "1.4",
		"url":      "golang.org",
		"language": "Go",
	}

	_, err1 := d.InsertToCollection("Matrix", data)
	c.Assert(err1, IsNil)

	var indexes []string
	indexes = []string{"version", "url", "language"}
	err2 := d.InsertIndexToCollection("Matrix", indexes)
	c.Assert(err2, IsNil)
}
