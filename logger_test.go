/*
 * Minio Cloud Storage (C) 2015 Minio, Inc.
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

package main

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/minio/minio-xl/pkg/probe"

	. "gopkg.in/check.v1"
)

type LoggerSuite struct{}

var _ = Suite(&LoggerSuite{})

func (s *LoggerSuite) TestLogger(c *C) {
	var buffer bytes.Buffer
	var fields logrus.Fields
	log.Out = &buffer
	log.Formatter = new(logrus.JSONFormatter)

	errorIf(probe.NewError(errors.New("Fake error")), "Failed with error.", nil)
	err := json.Unmarshal(buffer.Bytes(), &fields)
	c.Assert(err, IsNil)
	c.Assert(fields["level"], Equals, "error")

	msg, ok := fields["Error"]
	c.Assert(ok, Equals, true)
	c.Assert(msg.(map[string]interface{})["cause"], Equals, "Fake error")
}
