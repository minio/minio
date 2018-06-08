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
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"testing"
)

func TestValidateBucketName(t *testing.T) {
	testCases := []struct {
		bucketName string
		expectErr  bool
	}{
		{"lol", false},
		{"1-this-is-valid", false},
		{"1-this-too-is-valid-1", false},
		{"this.works.too.1", false},
		{"1234567", false},
		{"123", false},
		{"s3-eu-west-1.amazonaws.com", false},
		{"ideas-are-more-powerful-than-guns", false},
		{"testbucket", false},
		{"1bucket", false},
		{"bucket1", false},
		{"a.b", false},
		{"ab.a.bc", false},
		{"------", true},
		{"my..bucket", true},
		{"192.168.1.1", true},
		{"$this-is-not-valid-too", true},
		{"contains-$-dollar", true},
		{"contains-^-carret", true},
		{"......", true},
		{"", true},
		{"a", true},
		{"ab", true},
		{".starts-with-a-dot", true},
		{"ends-with-a-dot.", true},
		{"ends-with-a-dash-", true},
		{"-starts-with-a-dash", true},
		{"THIS-BEGINS-WITH-UPPERCASe", true},
		{"tHIS-ENDS-WITH-UPPERCASE", true},
		{"ThisBeginsAndEndsWithUpperCasE", true},
		{"una ñina", true},
		{"dash-.may-not-appear-next-to-dot", true},
		{"dash.-may-not-appear-next-to-dot", true},
		{"dash-.-may-not-appear-next-to-dot", true},
		{"lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", true},
		{"abc.def.-gh", true},
	}

	for i, testCase := range testCases {
		err := ValidateBucketName(testCase.bucketName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestValidateCompatBucketName(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		bucketName string
		expectErr  bool
	}{
		{"lol", false},
		{"1-this-is-valid", false},
		{"1-this-too-is-valid-1", false},
		{"this.works.too.1", false},
		{"1234567", false},
		{"123", false},
		{"s3-eu-west-1.amazonaws.com", false},
		{"ideas-are-more-powerful-than-guns", false},
		{"testbucket", false},
		{"1bucket", false},
		{"bucket1", false},
		{"a.b", false},
		{"ab.a.bc", false},
		{"------", true},
		{"my..bucket", false},
		{"192.168.1.1", false},
		{"$this-is-not-valid-too", true},
		{"contains-$-dollar", true},
		{"contains-^-carret", true},
		{"......", true},
		{"", true},
		{"a", true},
		{"ab", true},
		{".starts-with-a-dot", true},
		{"ends-with-a-dot.", true},
		{"ends-with-a-dash-", true},
		{"-starts-with-a-dash", true},
		{"THIS-BEGINS-WITH-UPPERCASe", false},
		{"tHIS-ENDS-WITH-UPPERCASE", false},
		{"ThisBeginsAndEndsWithUpperCasE", false},
		{"una ñina", true},
		{"dash-.may-not-appear-next-to-dot", false},
		{"dash.-may-not-appear-next-to-dot", false},
		{"dash-.-may-not-appear-next-to-dot", false},
		{"lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", false},
		{"abc.def.-gh", false},
	}

	for i, testCase := range testCases {
		err := validateCompatBucketName(testCase.bucketName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestValidateS3ObjectKey(t *testing.T) {
	testCases := []struct {
		key       string
		expectErr bool
	}{
		{"object", false},
		{"The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", false},
		{"f*le", false},
		{"contains-^-carret", false},
		{"contains-|-pipe", false},
		{"contains-\"-quote", false},
		{"contains-`-tick", false},
		{"..test", false},
		{".. test", false},
		{". test", false},
		{".test", false},
		{"There are far too many object names, and far too few bucket names!", false},
		{"", false},
		{"a/b/c/", false},
		{"/a/b/c", false},
		{"../../etc", false},
		{"../../", false},
		{"/../../etc", false},
		{" ../etc", false},
		{"./././", false},
		{"./etc", false},
		{`contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), true},
		{`object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		err := validateS3ObjectKey(testCase.key)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestValidateObjectKey(t *testing.T) {
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
	}

	testCases := []struct {
		key       string
		expectErr bool
	}{
		{"object", false},
		{"The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", false},
		{"f*le", errorOnWindows},
		{"contains-^-carret", false},
		{"contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", errorOnWindows},
		{"contains-`-tick", false},
		{"..test", false},
		{".. test", false},
		{". test", false},
		{".test", false},
		{"There are far too many object names, and far too few bucket names!", false},
		{"", false},
		{"a/b/c/", false},
		{"/a/b/c", true},
		{"../../etc", true},
		{"../../", true},
		{"/../../etc", true},
		{" ../etc", false},
		{"./././", true},
		{"./etc", true},
		{`contains-\-backslash`, true},
		{string([]byte{0xff, 0xfe, 0xfd}), true},
		{`object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		err := validateObjectKey(testCase.key)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestGetValidObjectKeyS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		key            string
		expectedResult string
		expectErr      bool
	}{

		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", "Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", "SHØRT", false},
		{"f*le", "f*le", false},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", false},
		{"contains-\"-quote", "contains-\"-quote", false},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", "There are far too many object names, and far too few bucket names!", false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "/a/b/c", false},
		{"../../etc", "../../etc", false},
		{"../../", "../../", false},
		{"/../../etc", "/../../etc", false},
		{" ../etc", " ../etc", false},
		{"./././", "./././", false},
		{"./etc", "./etc", false},
		{`contains-\-backslash`, `contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		result, err := getValidObjectKey(testCase.key)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testGetValidObjectKey(t *testing.T) {
	case3Result := "Cost Benefit Analysis (2009-2010).pptx"
	case4Result := "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A"
	case5Result := "SHØRT"
	case15Result := "There are far too many object names, and far too few bucket names!"
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
		if globalGatewayName == "" {
			case3Result = strings.ToLower(case3Result)
			case4Result = strings.ToLower(case4Result)
			case5Result = strings.ToLower(case5Result)
			case15Result = strings.ToLower(case15Result)
		}
	}

	testCases := []struct {
		key            string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", case3Result, false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", case4Result, false},
		{"SHØRT", case5Result, false},
		{"f*le", "f*le", errorOnWindows},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", "contains-\"-quote", errorOnWindows},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", case15Result, false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "", true},
		{"../../etc", "", true},
		{"../../", "", true},
		{"/../../etc", "", true},
		{" ../etc", " ../etc", false},
		{"./././", "", true},
		{"./etc", "", true},
		{`contains-\-backslash`, "", true},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		result, err := getValidObjectKey(testCase.key)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestGetValidObjectKeyNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testGetValidObjectKey(t)
}

func TestGetValidObjectKeyNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testGetValidObjectKey(t)
}

func TestRequestArgsCompatBucketNameS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		bucketName     string
		expectedResult string
		expectErr      bool
	}{
		{"lol", "lol", false},
		{"1-this-is-valid", "1-this-is-valid", false},
		{"1-this-too-is-valid-1", "1-this-too-is-valid-1", false},
		{"this.works.too.1", "this.works.too.1", false},
		{"1234567", "1234567", false},
		{"123", "123", false},
		{"s3-eu-west-1.amazonaws.com", "s3-eu-west-1.amazonaws.com", false},
		{"ideas-are-more-powerful-than-guns", "ideas-are-more-powerful-than-guns", false},
		{"testbucket", "testbucket", false},
		{"1bucket", "1bucket", false},
		{"bucket1", "bucket1", false},
		{"a.b", "a.b", false},
		{"ab.a.bc", "ab.a.bc", false},
		{"------", "", true},
		{"my..bucket", "my..bucket", false},
		{"192.168.1.1", "192.168.1.1", false},
		{"$this-is-not-valid-too", "", true},
		{"contains-$-dollar", "", true},
		{"contains-^-carret", "", true},
		{"......", "", true},
		{"", "", true},
		{"a", "", true},
		{"ab", "", true},
		{".starts-with-a-dot", "", true},
		{"ends-with-a-dot.", "", true},
		{"ends-with-a-dash-", "", true},
		{"-starts-with-a-dash", "", true},
		{"THIS-BEGINS-WITH-UPPERCASe", "THIS-BEGINS-WITH-UPPERCASe", false},
		{"tHIS-ENDS-WITH-UPPERCASE", "tHIS-ENDS-WITH-UPPERCASE", false},
		{"ThisBeginsAndEndsWithUpperCasE", "ThisBeginsAndEndsWithUpperCasE", false},
		{"una ñina", "", true},
		{"dash-.may-not-appear-next-to-dot", "dash-.may-not-appear-next-to-dot", false},
		{"dash.-may-not-appear-next-to-dot", "dash.-may-not-appear-next-to-dot", false},
		{"dash-.-may-not-appear-next-to-dot", "dash-.-may-not-appear-next-to-dot", false},
		{"lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", "lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", false},
		{"abc.def.-gh", "abc.def.-gh", false},
	}

	for i, testCase := range testCases {
		args := requestArgs{vars: map[string]string{"bucket": testCase.bucketName}}
		result, err := args.CompatBucketName()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testRequestArgsCompatBucketName(t *testing.T) {
	testCases := []struct {
		bucketName     string
		expectedResult string
		expectErr      bool
	}{
		{"lol", "lol", false},
		{"1-this-is-valid", "1-this-is-valid", false},
		{"1-this-too-is-valid-1", "1-this-too-is-valid-1", false},
		{"this.works.too.1", "this.works.too.1", false},
		{"1234567", "1234567", false},
		{"123", "123", false},
		{"s3-eu-west-1.amazonaws.com", "s3-eu-west-1.amazonaws.com", false},
		{"ideas-are-more-powerful-than-guns", "ideas-are-more-powerful-than-guns", false},
		{"testbucket", "testbucket", false},
		{"1bucket", "1bucket", false},
		{"bucket1", "bucket1", false},
		{"a.b", "a.b", false},
		{"ab.a.bc", "ab.a.bc", false},
		{"------", "", true},
		{"my..bucket", "", true},
		{"192.168.1.1", "", true},
		{"$this-is-not-valid-too", "", true},
		{"contains-$-dollar", "", true},
		{"contains-^-carret", "", true},
		{"......", "", true},
		{"", "", true},
		{"a", "", true},
		{"ab", "", true},
		{".starts-with-a-dot", "", true},
		{"ends-with-a-dot.", "", true},
		{"ends-with-a-dash-", "", true},
		{"-starts-with-a-dash", "", true},
		{"THIS-BEGINS-WITH-UPPERCASe", "", true},
		{"tHIS-ENDS-WITH-UPPERCASE", "", true},
		{"ThisBeginsAndEndsWithUpperCasE", "", true},
		{"una ñina", "", true},
		{"dash-.may-not-appear-next-to-dot", "", true},
		{"dash.-may-not-appear-next-to-dot", "", true},
		{"dash-.-may-not-appear-next-to-dot", "", true},
		{"lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", "", true},
		{"abc.def.-gh", "", true},
	}

	for i, testCase := range testCases {
		args := requestArgs{vars: map[string]string{"bucket": testCase.bucketName}}
		result, err := args.CompatBucketName()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsCompatBucketNameNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsCompatBucketName(t)
}

func TestRequestArgsCompatBucketNameNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsCompatBucketName(t)
}

func testRequestArgsBucketName(t *testing.T) {
	testCases := []struct {
		bucketName     string
		expectedResult string
		expectErr      bool
	}{
		{"lol", "lol", false},
		{"1-this-is-valid", "1-this-is-valid", false},
		{"1-this-too-is-valid-1", "1-this-too-is-valid-1", false},
		{"this.works.too.1", "this.works.too.1", false},
		{"1234567", "1234567", false},
		{"123", "123", false},
		{"s3-eu-west-1.amazonaws.com", "s3-eu-west-1.amazonaws.com", false},
		{"ideas-are-more-powerful-than-guns", "ideas-are-more-powerful-than-guns", false},
		{"testbucket", "testbucket", false},
		{"1bucket", "1bucket", false},
		{"bucket1", "bucket1", false},
		{"a.b", "a.b", false},
		{"ab.a.bc", "ab.a.bc", false},
		{"------", "", true},
		{"my..bucket", "", true},
		{"192.168.1.1", "", true},
		{"$this-is-not-valid-too", "", true},
		{"contains-$-dollar", "", true},
		{"contains-^-carret", "", true},
		{"......", "", true},
		{"", "", true},
		{"a", "", true},
		{"ab", "", true},
		{".starts-with-a-dot", "", true},
		{"ends-with-a-dot.", "", true},
		{"ends-with-a-dash-", "", true},
		{"-starts-with-a-dash", "", true},
		{"THIS-BEGINS-WITH-UPPERCASe", "", true},
		{"tHIS-ENDS-WITH-UPPERCASE", "", true},
		{"ThisBeginsAndEndsWithUpperCasE", "", true},
		{"una ñina", "", true},
		{"dash-.may-not-appear-next-to-dot", "", true},
		{"dash.-may-not-appear-next-to-dot", "", true},
		{"dash-.-may-not-appear-next-to-dot", "", true},
		{"lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", "", true},
		{"abc.def.-gh", "", true},
	}

	for i, testCase := range testCases {
		args := requestArgs{vars: map[string]string{"bucket": testCase.bucketName}}
		result, err := args.BucketName()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsBucketNameS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testRequestArgsBucketName(t)
}

func TestRequestArgsBucketNameNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsBucketName(t)
}

func TestRequestArgsBucketNameNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsBucketName(t)
}

func TestRequestArgsObjectNameS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		objectName     string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", "Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", "SHØRT", false},
		{"f*le", "f*le", false},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", false},
		{"contains-\"-quote", "contains-\"-quote", false},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", "There are far too many object names, and far too few bucket names!", false},
		{"", "", true},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "/a/b/c", false},
		{"../../etc", "../../etc", false},
		{"../../", "../../", false},
		{"/../../etc", "/../../etc", false},
		{" ../etc", " ../etc", false},
		{"./././", "./././", false},
		{"./etc", "./etc", false},
		{`contains-\-backslash`, `contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		args := requestArgs{vars: map[string]string{"object": testCase.objectName}}
		result, err := args.ObjectName()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testRequestArgsObjectName(t *testing.T) {
	case3Result := "Cost Benefit Analysis (2009-2010).pptx"
	case4Result := "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A"
	case5Result := "SHØRT"
	case15Result := "There are far too many object names, and far too few bucket names!"
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
		if globalGatewayName == "" {
			case3Result = strings.ToLower(case3Result)
			case4Result = strings.ToLower(case4Result)
			case5Result = strings.ToLower(case5Result)
			case15Result = strings.ToLower(case15Result)
		}
	}

	testCases := []struct {
		objectName     string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", case3Result, false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", case4Result, false},
		{"SHØRT", case5Result, false},
		{"f*le", "f*le", errorOnWindows},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", "contains-\"-quote", errorOnWindows},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", case15Result, false},
		{"", "", true},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "", true},
		{"../../etc", "", true},
		{"../../", "", true},
		{"/../../etc", "", true},
		{" ../etc", " ../etc", false},
		{"./././", "", true},
		{"./etc", "", true},
		{`contains-\-backslash`, "", true},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		args := requestArgs{vars: map[string]string{"object": testCase.objectName}}
		result, err := args.ObjectName()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsObjectNameNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsObjectName(t)
}

func TestRequestArgsObjectNameNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsObjectName(t)
}

func TestRequestArgsPrefixS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		prefix         string
		expectedResult string
		expectErr      bool
	}{

		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", "Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", "SHØRT", false},
		{"f*le", "f*le", false},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", false},
		{"contains-\"-quote", "contains-\"-quote", false},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", "There are far too many object names, and far too few bucket names!", false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "/a/b/c", false},
		{"../../etc", "../../etc", false},
		{"../../", "../../", false},
		{"/../../etc", "/../../etc", false},
		{" ../etc", " ../etc", false},
		{"./././", "./././", false},
		{"./etc", "./etc", false},
		{`contains-\-backslash`, `contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("prefix", testCase.prefix)
		args := requestArgs{queryValues: v}
		result, err := args.Prefix()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testRequestArgsPrefix(t *testing.T) {
	case3Result := "Cost Benefit Analysis (2009-2010).pptx"
	case4Result := "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A"
	case5Result := "SHØRT"
	case15Result := "There are far too many object names, and far too few bucket names!"
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
		if globalGatewayName == "" {
			case3Result = strings.ToLower(case3Result)
			case4Result = strings.ToLower(case4Result)
			case5Result = strings.ToLower(case5Result)
			case15Result = strings.ToLower(case15Result)
		}
	}

	testCases := []struct {
		prefix         string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", case3Result, false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", case4Result, false},
		{"SHØRT", case5Result, false},
		{"f*le", "f*le", errorOnWindows},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", "contains-\"-quote", errorOnWindows},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", case15Result, false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "", true},
		{"../../etc", "", true},
		{"../../", "", true},
		{"/../../etc", "", true},
		{" ../etc", " ../etc", false},
		{"./././", "", true},
		{"./etc", "", true},
		{`contains-\-backslash`, "", true},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("prefix", testCase.prefix)
		args := requestArgs{queryValues: v}
		result, err := args.Prefix()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsPrefixNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsPrefix(t)
}

func TestRequestArgsPrefixNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsPrefix(t)
}

func TestRequestArgsStartAfterS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		startAfter     string
		expectedResult string
		expectErr      bool
	}{

		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", "Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", "SHØRT", false},
		{"f*le", "f*le", false},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", false},
		{"contains-\"-quote", "contains-\"-quote", false},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", "There are far too many object names, and far too few bucket names!", false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "/a/b/c", false},
		{"../../etc", "../../etc", false},
		{"../../", "../../", false},
		{"/../../etc", "/../../etc", false},
		{" ../etc", " ../etc", false},
		{"./././", "./././", false},
		{"./etc", "./etc", false},
		{`contains-\-backslash`, `contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("start-after", testCase.startAfter)
		args := requestArgs{queryValues: v}
		result, err := args.StartAfter()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testRequestArgsStartAfter(t *testing.T) {
	case3Result := "Cost Benefit Analysis (2009-2010).pptx"
	case4Result := "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A"
	case5Result := "SHØRT"
	case15Result := "There are far too many object names, and far too few bucket names!"
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
		if globalGatewayName == "" {
			case3Result = strings.ToLower(case3Result)
			case4Result = strings.ToLower(case4Result)
			case5Result = strings.ToLower(case5Result)
			case15Result = strings.ToLower(case15Result)
		}
	}

	testCases := []struct {
		startAfter     string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", case3Result, false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", case4Result, false},
		{"SHØRT", case5Result, false},
		{"f*le", "f*le", errorOnWindows},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", "contains-\"-quote", errorOnWindows},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", case15Result, false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "", true},
		{"../../etc", "", true},
		{"../../", "", true},
		{"/../../etc", "", true},
		{" ../etc", " ../etc", false},
		{"./././", "", true},
		{"./etc", "", true},
		{`contains-\-backslash`, "", true},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("start-after", testCase.startAfter)
		args := requestArgs{queryValues: v}
		result, err := args.StartAfter()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsStartAfterNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsStartAfter(t)
}

func TestRequestArgsStartAfterNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsStartAfter(t)
}

func TestRequestArgsMarkerS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		marker         string
		expectedResult string
		expectErr      bool
	}{

		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", "Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", "SHØRT", false},
		{"f*le", "f*le", false},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", false},
		{"contains-\"-quote", "contains-\"-quote", false},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", "There are far too many object names, and far too few bucket names!", false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "/a/b/c", false},
		{"../../etc", "../../etc", false},
		{"../../", "../../", false},
		{"/../../etc", "/../../etc", false},
		{" ../etc", " ../etc", false},
		{"./././", "./././", false},
		{"./etc", "./etc", false},
		{`contains-\-backslash`, `contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("marker", testCase.marker)
		args := requestArgs{queryValues: v}
		result, err := args.Marker()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testRequestArgsMarker(t *testing.T) {
	case3Result := "Cost Benefit Analysis (2009-2010).pptx"
	case4Result := "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A"
	case5Result := "SHØRT"
	case15Result := "There are far too many object names, and far too few bucket names!"
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
		if globalGatewayName == "" {
			case3Result = strings.ToLower(case3Result)
			case4Result = strings.ToLower(case4Result)
			case5Result = strings.ToLower(case5Result)
			case15Result = strings.ToLower(case15Result)
		}
	}

	testCases := []struct {
		marker         string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", case3Result, false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", case4Result, false},
		{"SHØRT", case5Result, false},
		{"f*le", "f*le", errorOnWindows},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", "contains-\"-quote", errorOnWindows},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", case15Result, false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "", true},
		{"../../etc", "", true},
		{"../../", "", true},
		{"/../../etc", "", true},
		{" ../etc", " ../etc", false},
		{"./././", "", true},
		{"./etc", "", true},
		{`contains-\-backslash`, "", true},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("marker", testCase.marker)
		args := requestArgs{queryValues: v}
		result, err := args.Marker()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsMarkerNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsMarker(t)
}

func TestRequestArgsMarkerNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsMarker(t)
}

func TestRequestArgsKeyMarkerS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testCases := []struct {
		keyMarker      string
		expectedResult string
		expectErr      bool
	}{

		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", false},
		{"Cost Benefit Analysis (2009-2010).pptx", "Cost Benefit Analysis (2009-2010).pptx", false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", false},
		{"SHØRT", "SHØRT", false},
		{"f*le", "f*le", false},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", false},
		{"contains-\"-quote", "contains-\"-quote", false},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", "There are far too many object names, and far too few bucket names!", false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "/a/b/c", false},
		{"../../etc", "../../etc", false},
		{"../../", "../../", false},
		{"/../../etc", "/../../etc", false},
		{" ../etc", " ../etc", false},
		{"./././", "./././", false},
		{"./etc", "./etc", false},
		{`contains-\-backslash`, `contains-\-backslash`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, false},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("key-marker", testCase.keyMarker)
		args := requestArgs{queryValues: v}
		result, err := args.KeyMarker()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func testRequestArgsKeyMarker(t *testing.T) {
	case3Result := "Cost Benefit Analysis (2009-2010).pptx"
	case4Result := "117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A"
	case5Result := "SHØRT"
	case15Result := "There are far too many object names, and far too few bucket names!"
	errorOnWindows := false
	if runtime.GOOS == "windows" {
		errorOnWindows = true
		if globalGatewayName == "" {
			case3Result = strings.ToLower(case3Result)
			case4Result = strings.ToLower(case4Result)
			case5Result = strings.ToLower(case5Result)
			case15Result = strings.ToLower(case15Result)
		}
	}

	testCases := []struct {
		keyMarker      string
		expectedResult string
		expectErr      bool
	}{
		{"object", "object", false},
		{"The Shining Script <v1>.pdf", "The Shining Script <v1>.pdf", errorOnWindows},
		{"Cost Benefit Analysis (2009-2010).pptx", case3Result, false},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", case4Result, false},
		{"SHØRT", case5Result, false},
		{"f*le", "f*le", errorOnWindows},
		{"contains-^-carret", "contains-^-carret", false},
		{"contains-|-pipe", "contains-|-pipe", errorOnWindows},
		{"contains-\"-quote", "contains-\"-quote", errorOnWindows},
		{"contains-`-tick", "contains-`-tick", false},
		{"..test", "..test", false},
		{".. test", ".. test", false},
		{". test", ". test", false},
		{".test", ".test", false},
		{"There are far too many object names, and far too few bucket names!", case15Result, false},
		{"", "", false},
		{"a/b/c/", "a/b/c/", false},
		{"/a/b/c", "", true},
		{"../../etc", "", true},
		{"../../", "", true},
		{"/../../etc", "", true},
		{" ../etc", " ../etc", false},
		{"./././", "", true},
		{"./etc", "", true},
		{`contains-\-backslash`, "", true},
		{string([]byte{0xff, 0xfe, 0xfd}), "", true},
		{`object-name-with-:*?"<>|-characters`, `object-name-with-:*?"<>|-characters`, errorOnWindows},
	}

	for i, testCase := range testCases {
		v := make(url.Values)
		v.Add("key-marker", testCase.keyMarker)
		args := requestArgs{queryValues: v}
		result, err := args.KeyMarker()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsKeyMarkerNonS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "azure"

	testRequestArgsKeyMarker(t)
}

func TestRequestArgsKeyMarkerNonGateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = ""

	testRequestArgsKeyMarker(t)
}

func TestRequestArgsCopySource(t *testing.T) {
	h := make(http.Header)
	args := requestArgs{header: h}
	if _, _, err := args.CopySource(); err == nil {
		t.Fatalf("error: expected: <error>, got: <nil>")
	}

	testCases := []struct {
		copySource         string
		expectedBucketName string
		expectedObjectName string
		expectErr          bool
	}{
		{"/mybucket/path/to/object", "mybucket", "path/to/object", false},
		{"/mybucket/path/to/object/", "mybucket", "path/to/object/", false},
		{"mybucket/path/to/object", "mybucket", "path/to/object", false},
		// source is missing with object name.
		{"/mybucket", "", "", true},
		// invalid bucket name.
		{"/ab/path/to/object", "", "", true},
		// empty object name in source.
		{"/mybucket/", "", "", true},
		// invalid object name.
		{"/mybucket/" + string([]byte{0xff, 0xfe, 0xfd}), "", "", true},
	}

	for i, testCase := range testCases {
		h = make(http.Header)
		h.Add("X-Amz-Copy-Source", testCase.copySource)
		args = requestArgs{header: h}
		bucketName, objectName, err := args.CopySource()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if bucketName != testCase.expectedBucketName {
				t.Fatalf("case %v: bucket name: expected: %v, got: %v", i+1, testCase.expectedBucketName, bucketName)
			}
			if objectName != testCase.expectedObjectName {
				t.Fatalf("case %v: object name: expected: %v, got: %v", i+1, testCase.expectedObjectName, objectName)
			}
		}
	}
}

func TestRequestArgsMetadataDirective(t *testing.T) {
	h := make(http.Header)
	case1Args := requestArgs{header: h}

	h = make(http.Header)
	h.Add("X-Amz-Metadata-Directive", "COPY")
	case2Args := requestArgs{header: h}

	h = make(http.Header)
	h.Add("X-Amz-Metadata-Directive", "REPLACE")
	case3Args := requestArgs{header: h}

	h = make(http.Header)
	h.Add("X-Amz-Metadata-Directive", "")
	case4Args := requestArgs{header: h}

	h = make(http.Header)
	h.Add("X-Amz-Metadata-Directive", "COPY/REPLACE")
	case5Args := requestArgs{header: h}

	testCases := []struct {
		args           requestArgs
		expectedResult string
		expectErr      bool
	}{
		{case1Args, "COPY", false},
		{case2Args, "COPY", false},
		{case3Args, "REPLACE", false},
		{case4Args, "", true},
		{case5Args, "", true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.MetadataDirective()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsUploadID(t *testing.T) {
	v := make(url.Values)
	v.Add("uploadId", "c571287c-0622-44b2-80a9-79dc5ffa9679")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("uploadId", "VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("uploadId", "uploadId")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("uploadId", "")
	case5Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult string
		expectErr      bool
	}{
		{case1Args, "c571287c-0622-44b2-80a9-79dc5ffa9679", false},
		{case2Args, "VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR", false},
		{case3Args, "uploadId", false},
		{case4Args, "", true},
		{case5Args, "", true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.UploadID()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsPartNumber(t *testing.T) {
	v := make(url.Values)
	v.Add("partNumber", "1234")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("partNumber", "12345")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("partNumber", "0")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("partNumber", "-10")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("partNumber", "seven")
	case5Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case6Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("partNumber", "")
	case7Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult int
		expectErr      bool
	}{
		{case1Args, 1234, false},
		{case2Args, 12345, false},
		{case3Args, 0, true},
		{case4Args, 0, true},
		{case5Args, 0, true},
		{case6Args, 0, true},
		{case7Args, 0, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.PartNumber()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsPartNumberMarker(t *testing.T) {
	v := make(url.Values)
	v.Add("part-number-marker", "1")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("part-number-marker", "10000")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("part-number-marker", "")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("part-number-marker", "12345")
	case5Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("part-number-marker", "seven")
	case6Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("part-number-marker", "0")
	case7Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("part-number-marker", "-10")
	case8Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult int
		expectErr      bool
	}{
		{case1Args, 1, false},
		{case2Args, 10000, false},
		{case3Args, 0, false},
		{case4Args, 0, false},
		{case5Args, 0, true},
		{case6Args, 0, true},
		{case7Args, 0, true},
		{case8Args, 0, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.PartNumberMarker()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsContinuationToken(t *testing.T) {
	v := make(url.Values)
	v.Add("continuation-token", "c571287c-0622-44b2-80a9-79dc5ffa9679")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("continuation-token", "VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("continuation-token", `$$%^&*`)
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("continuation-token", "")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case5Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult string
	}{
		{case1Args, "c571287c-0622-44b2-80a9-79dc5ffa9679"},
		{case2Args, "VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR"},
		{case3Args, `$$%^&*`},
		{case4Args, ""},
		{case5Args, ""},
	}

	for i, testCase := range testCases {
		result := testCase.args.ContinuationToken()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRequestArgsFetchOwner(t *testing.T) {
	v := make(url.Values)
	v.Add("fetch-owner", "true")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("fetch-owner", "false")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("fetch-owner", "True")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("fetch-owner", "")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case5Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult bool
	}{
		{case1Args, true},
		{case2Args, false},
		{case3Args, false},
		{case4Args, false},
		{case5Args, false},
	}

	for i, testCase := range testCases {
		result := testCase.args.FetchOwner()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRequestArgsUploadIDMarker(t *testing.T) {
	v := make(url.Values)
	v.Add("upload-id-marker", "1234")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("upload-id-marker", "VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("upload-id-marker", `$$%^&*`)
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("upload-id-marker", "")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case5Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult string
	}{
		{case1Args, "1234"},
		{case2Args, "VCVsb2FkIElEIGZvciBlbZZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZR"},
		{case3Args, `$$%^&*`},
		{case4Args, ""},
		{case5Args, ""},
	}

	for i, testCase := range testCases {
		result := testCase.args.UploadIDMarker()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func testRequestArgsDelimiter(t *testing.T) {
	v := make(url.Values)
	v.Add("delimiter", "/")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("delimiter", "")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("delimiter", "Dem")
	case4Args := requestArgs{queryValues: v}
	case4ExpectedResult := ""
	case4ExpectErr := true
	if globalGatewayName == "s3" {
		case4ExpectedResult = "Dem"
		case4ExpectErr = false
	}

	testCases := []struct {
		args           requestArgs
		expectedResult string
		expectErr      bool
	}{
		{case1Args, "/", false},
		{case2Args, "", false},
		{case3Args, "", false},
		{case4Args, case4ExpectedResult, case4ExpectErr},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.Delimiter()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsDelimiter(t *testing.T) {
	testRequestArgsDelimiter(t)
}

func TestRequestArgsDelimiterS3Gateway(t *testing.T) {
	tmpGlobalGatewayName := globalGatewayName
	defer func() {
		globalGatewayName = tmpGlobalGatewayName
	}()
	globalGatewayName = "s3"

	testRequestArgsDelimiter(t)
}

func TestRequestArgsMaxParts(t *testing.T) {
	v := make(url.Values)
	v.Add("max-parts", "1")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-parts", "1000")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-parts", "0")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-parts", "12345")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case5Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-parts", "")
	case6Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-parts", "-10")
	case7Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-parts", "seventy")
	case8Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult int
		expectErr      bool
	}{
		{case1Args, 1, false},
		{case2Args, 1000, false},
		{case3Args, 1000, false},
		{case4Args, 1000, false},
		{case5Args, 1000, false},
		{case6Args, 1000, false},
		{case7Args, 0, true},
		{case8Args, 0, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.MaxParts()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsMaxKeys(t *testing.T) {
	v := make(url.Values)
	v.Add("max-keys", "1")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-keys", "1000")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-keys", "0")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-keys", "12345")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case5Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-keys", "")
	case6Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-keys", "-10")
	case7Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-keys", "seventy")
	case8Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult int
		expectErr      bool
	}{
		{case1Args, 1, false},
		{case2Args, 1000, false},
		{case3Args, 1000, false},
		{case4Args, 1000, false},
		{case5Args, 1000, false},
		{case6Args, 1000, false},
		{case7Args, 0, true},
		{case8Args, 0, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.MaxKeys()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestRequestArgsMaxUploads(t *testing.T) {
	v := make(url.Values)
	v.Add("max-uploads", "1")
	case1Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-uploads", "1000")
	case2Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-uploads", "0")
	case3Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-uploads", "12345")
	case4Args := requestArgs{queryValues: v}

	v = make(url.Values)
	case5Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-uploads", "")
	case6Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-uploads", "-10")
	case7Args := requestArgs{queryValues: v}

	v = make(url.Values)
	v.Add("max-uploads", "seventy")
	case8Args := requestArgs{queryValues: v}

	testCases := []struct {
		args           requestArgs
		expectedResult int
		expectErr      bool
	}{
		{case1Args, 1, false},
		{case2Args, 1000, false},
		{case3Args, 1000, false},
		{case4Args, 1000, false},
		{case5Args, 1000, false},
		{case6Args, 1000, false},
		{case7Args, 0, true},
		{case8Args, 0, true},
	}

	for i, testCase := range testCases {
		result, err := testCase.args.MaxUploads()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if result != testCase.expectedResult {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}
