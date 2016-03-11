/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package fs

import (
	"testing"
)

func ensureBucketName(name string, t *testing.T, pass bool) {
	if pass && IsValidBucketName(name) {
		return
	}
	if !pass && !IsValidBucketName(name) {
		return
	}

	t.Errorf("\"%s\" should have passed [%t]\n", name, pass)
}

func TestIsValidBucketName(t *testing.T) {
	ensureBucketName("lol", t, true)
	ensureBucketName("s3-eu-west-1.amazonaws.com", t, true)
	ensureBucketName("ideas-are-more-powerful-than-guns", t, true)
	ensureBucketName("testbucket", t, true)
	ensureBucketName("1bucket", t, true)
	ensureBucketName("bucket1", t, true)

	ensureBucketName("testing.", t, false)
	ensureBucketName("", t, false)
	ensureBucketName("......", t, false)
	ensureBucketName("THIS-IS-UPPERCASE", t, false)
	ensureBucketName("una ñina", t, false)
	ensureBucketName("lalalallalallalalalallalallalallalallalallalallalallalallallalala", t, false)
}

func ensureObjectName(name string, t *testing.T, pass bool) {
	if pass && IsValidObjectName(name) {
		return
	}
	if !pass && !IsValidObjectName(name) {
		return
	}

	t.Errorf("\"%s\" should have passed [%t]\n", name, pass)
}

func TestIsValidObjectName(t *testing.T) {
	ensureObjectName("object", t, true)
	ensureObjectName("The Shining Script <v1>.pdf", t, true)
	ensureObjectName("Cost Benefit Analysis (2009-2010).pptx", t, true)
	ensureObjectName("117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", t, true)
	ensureObjectName("SHØRT", t, true)
	ensureObjectName("There are far too many object names, and far too few bucket names!", t, true)

	ensureObjectName("", t, false)
	// Bad UTF8 strings should not pass.
	ensureObjectName(string([]byte{0xff, 0xfe, 0xfd}), t, false)
}
