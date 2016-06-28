/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	. "gopkg.in/check.v1"
)

type ObjectLayerAPISuite struct{}

var _ = Suite(&ObjectLayerAPISuite{})

// TestFSAPISuite - Calls object layer suite tests with FS backend.
func (s *ObjectLayerAPISuite) TestFSAPISuite(c *C) {
	// Initialize name space lock.
	initNSLock()
	// function which creates a temp FS backend and executes the object layer suite test.
	execObjectLayerSuiteTestFS := func(objSuiteTest objSuiteTestType) {
		// create temp object layer backend.
		// returns the disk and FS object layer.
		objLayer, fsDisk, err := makeTestBackend("FS")
		c.Check(err, IsNil)
		// remove the disks.
		defer removeRoots(fsDisk)
		// execute the object layer suite tests.
		objSuiteTest(c, objLayer)
	}
	// APITestSuite contains set of all object layer suite test.
	// These set of test functions are called here.
	APITestSuite(c, execObjectLayerSuiteTestFS)
}

// type for object layer suites tests.
type objSuiteTestType func(c *C, obj ObjectLayer)

// TestXLAPISuite - Calls object layer suite tests with XL backend.
func (s *ObjectLayerAPISuite) TestXLAPISuite(c *C) {
	// Initialize name space lock.
	initNSLock()
	// function which creates a temp XL backend and executes the object layer suite test.
	execObjectLayerSuiteTestXL := func(objSuiteTest objSuiteTestType) {
		// create temp object layer backend.
		// returns the disk and XL object layer.
		objLayer, erasureDisks, err := makeTestBackend("XL")
		c.Check(err, IsNil)
		// remove the disks.
		defer removeRoots(erasureDisks)
		// execute the object layer suite tests.
		objSuiteTest(c, objLayer)
	}
	// APITestSuite contains set of all object layer suite test.
	// These set of test functions are called here.
	APITestSuite(c, execObjectLayerSuiteTestXL)

}
