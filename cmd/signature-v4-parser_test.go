// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// generates credential string from its fields.
func generateCredentialStr(accessKey, date, region, service, requestVersion string) string {
	return "Credential=" + joinWithSlash(accessKey, date, region, service, requestVersion)
}

// joins the argument strings with a '/' and returns it.
func joinWithSlash(accessKey, date, region, service, requestVersion string) string {
	return strings.Join([]string{
		accessKey,
		date,
		region,
		service,
		requestVersion,
	}, SlashSeparator)
}

// generate CredentialHeader from its fields.
func generateCredentials(t *testing.T, accessKey string, date string, region, service, requestVersion string) credentialHeader {
	cred := credentialHeader{
		accessKey: accessKey,
	}
	parsedDate, err := time.Parse(yyyymmdd, date)
	if err != nil {
		t.Fatalf("Failed to parse date")
	}
	cred.scope.date = parsedDate
	cred.scope.region = region
	cred.scope.service = service
	cred.scope.request = requestVersion

	return cred
}

// validates the credential fields against the expected credential.
func validateCredentialfields(t *testing.T, testNum int, expectedCredentials credentialHeader, actualCredential credentialHeader) {
	if expectedCredentials.accessKey != actualCredential.accessKey {
		t.Errorf("Test %d: AccessKey mismatch: Expected \"%s\", got \"%s\"", testNum, expectedCredentials.accessKey, actualCredential.accessKey)
	}
	if !expectedCredentials.scope.date.Equal(actualCredential.scope.date) {
		t.Errorf("Test %d: Date mismatch:Expected \"%s\", got \"%s\"", testNum, expectedCredentials.scope.date, actualCredential.scope.date)
	}
	if expectedCredentials.scope.region != actualCredential.scope.region {
		t.Errorf("Test %d: region mismatch:Expected \"%s\", got \"%s\"", testNum, expectedCredentials.scope.region, actualCredential.scope.region)
	}
	if expectedCredentials.scope.service != actualCredential.scope.service {
		t.Errorf("Test %d: service mismatch:Expected \"%s\", got \"%s\"", testNum, expectedCredentials.scope.service, actualCredential.scope.service)
	}

	if expectedCredentials.scope.request != actualCredential.scope.request {
		t.Errorf("Test %d: scope request mismatch:Expected \"%s\", got \"%s\"", testNum, expectedCredentials.scope.request, actualCredential.scope.request)
	}
}

// TestParseCredentialHeader - validates the format validator and extractor for the Credential header in an aws v4 request.
// A valid format of credential should be of the following format.
// Credential = accessKey + SlashSeparator+ scope
// where scope = string.Join([]string{  currTime.Format(yyyymmdd),
//
//				globalMinioDefaultRegion,
//	              	"s3",
//			        "aws4_request",
//	                      },SlashSeparator)
func TestParseCredentialHeader(t *testing.T) {
	sampleTimeStr := UTCNow().Format(yyyymmdd)

	testCases := []struct {
		inputCredentialStr  string
		expectedCredentials credentialHeader
		expectedErrCode     APIErrorCode
	}{
		// Test Case - 1.
		// Test case with no '=' in te inputCredentialStr.
		{
			inputCredentialStr:  "Credential",
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrMissingFields,
		},
		// Test Case - 2.
		// Test case with no "Credential" string in te inputCredentialStr.
		{
			inputCredentialStr:  "Cred=",
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrMissingCredTag,
		},
		// Test Case - 3.
		// Test case with malformed credentials.
		{
			inputCredentialStr:  "Credential=abc",
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrCredMalformed,
		},
		// Test Case - 4.
		// Test case with AccessKey of length 2.
		{
			inputCredentialStr: generateCredentialStr(
				"^#",
				UTCNow().Format(yyyymmdd),
				"ABCD",
				"ABCD",
				"ABCD"),
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrInvalidAccessKeyID,
		},
		// Test Case - 5.
		// Test case with invalid date format date.
		// a valid date format for credentials is "yyyymmdd".
		{
			inputCredentialStr: generateCredentialStr(
				"Z7IXGOO6BZ0REAN1Q26I",
				UTCNow().String(),
				"ABCD",
				"ABCD",
				"ABCD"),
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrMalformedCredentialDate,
		},
		// Test Case - 6.
		// Test case with invalid service.
		// "s3" is the valid service string.
		{
			inputCredentialStr: generateCredentialStr(
				"Z7IXGOO6BZ0REAN1Q26I",
				UTCNow().Format(yyyymmdd),
				"us-west-1",
				"ABCD",
				"ABCD"),
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrInvalidServiceS3,
		},
		// Test Case - 7.
		// Test case with invalid region.
		{
			inputCredentialStr: generateCredentialStr(
				"Z7IXGOO6BZ0REAN1Q26I",
				UTCNow().Format(yyyymmdd),
				"us-west-2",
				"s3",
				"aws4_request"),
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrAuthorizationHeaderMalformed,
		},
		// Test Case - 8.
		// Test case with invalid request version.
		// "aws4_request" is the valid request version.
		{
			inputCredentialStr: generateCredentialStr(
				"Z7IXGOO6BZ0REAN1Q26I",
				UTCNow().Format(yyyymmdd),
				"us-west-1",
				"s3",
				"ABCD"),
			expectedCredentials: credentialHeader{},
			expectedErrCode:     ErrInvalidRequestVersion,
		},
		// Test Case - 9.
		// Test case with right inputs. Expected to return a valid CredentialHeader.
		// "aws4_request" is the valid request version.
		{
			inputCredentialStr: generateCredentialStr(
				"Z7IXGOO6BZ0REAN1Q26I",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedCredentials: generateCredentials(
				t,
				"Z7IXGOO6BZ0REAN1Q26I",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedErrCode: ErrNone,
		},
		// Test Case - 10.
		// Test case with right inputs -> AccessKey contains `/`. See minio/#6443
		// "aws4_request" is the valid request version.
		{
			inputCredentialStr: generateCredentialStr(
				"LOCALKEY/DEV/1",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedCredentials: generateCredentials(
				t,
				"LOCALKEY/DEV/1",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedErrCode: ErrNone,
		},
		// Test Case - 11.
		// Test case with right inputs -> AccessKey contains `=`. See minio/#7376
		// "aws4_request" is the valid request version.
		{
			inputCredentialStr: generateCredentialStr(
				"LOCALKEY/DEV/1=",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedCredentials: generateCredentials(
				t,
				"LOCALKEY/DEV/1=",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedErrCode: ErrNone,
		},
		// Test Case - 12.
		// Test case with right inputs but trailing `/`. Expected to return a valid CredentialHeader.
		// "aws4_request" is the valid request version.
		{
			inputCredentialStr: generateCredentialStr(
				"Z7IXGOO6BZ0REAN1Q26I",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request/"),
			expectedCredentials: generateCredentials(
				t,
				"Z7IXGOO6BZ0REAN1Q26I",
				sampleTimeStr,
				"us-west-1",
				"s3",
				"aws4_request"),
			expectedErrCode: ErrNone,
		},
	}

	for i, testCase := range testCases {
		actualCredential, actualErrCode := parseCredentialHeader(testCase.inputCredentialStr, "us-west-1", "s3")
		// validating the credential fields.
		if testCase.expectedErrCode != actualErrCode {
			t.Fatalf("Test %d: Expected the APIErrCode to be %s, got %s", i+1, errorCodes[testCase.expectedErrCode].Code, errorCodes[actualErrCode].Code)
		}
		if actualErrCode == ErrNone {
			validateCredentialfields(t, i+1, testCase.expectedCredentials, actualCredential)
		}
	}
}

// TestParseSignature - validates the logic for extracting the signature string.
func TestParseSignature(t *testing.T) {
	testCases := []struct {
		inputSignElement string
		expectedSignStr  string
		expectedErrCode  APIErrorCode
	}{
		// Test case - 1.
		// SignElement doesn't have 2 parts on an attempt to split at '='.
		// ErrMissingFields expected.
		{
			inputSignElement: "Signature",
			expectedSignStr:  "",
			expectedErrCode:  ErrMissingFields,
		},
		// Test case - 2.
		// SignElement does have 2 parts but doesn't have valid signature value.
		// ErrMissingFields expected.
		{
			inputSignElement: "Signature=",
			expectedSignStr:  "",
			expectedErrCode:  ErrMissingFields,
		},
		// Test case - 3.
		// SignElement with missing "SignatureTag",ErrMissingSignTag expected.
		{
			inputSignElement: "Sign=",
			expectedSignStr:  "",
			expectedErrCode:  ErrMissingSignTag,
		},
		// Test case - 4.
		// Test case with valid inputs.
		{
			inputSignElement: "Signature=abcd",
			expectedSignStr:  "abcd",
			expectedErrCode:  ErrNone,
		},
	}
	for i, testCase := range testCases {
		actualSignStr, actualErrCode := parseSignature(testCase.inputSignElement)
		if testCase.expectedErrCode != actualErrCode {
			t.Fatalf("Test %d: Expected the APIErrCode to be %d, got %d", i+1, testCase.expectedErrCode, actualErrCode)
		}
		if actualErrCode == ErrNone {
			if testCase.expectedSignStr != actualSignStr {
				t.Errorf("Test %d: Expected the result to be \"%s\", but got \"%s\". ", i+1, testCase.expectedSignStr, actualSignStr)
			}
		}
	}
}

// TestParseSignedHeaders - validates the logic for extracting the signature string.
func TestParseSignedHeaders(t *testing.T) {
	testCases := []struct {
		inputSignElement      string
		expectedSignedHeaders []string
		expectedErrCode       APIErrorCode
	}{
		// Test case - 1.
		// SignElement doesn't have 2 parts on an attempt to split at '='.
		// ErrMissingFields expected.
		{
			inputSignElement:      "SignedHeaders",
			expectedSignedHeaders: nil,
			expectedErrCode:       ErrMissingFields,
		},
		// Test case - 2.
		// SignElement with missing "SigHeaderTag",ErrMissingSignHeadersTag expected.
		{
			inputSignElement:      "Sign=",
			expectedSignedHeaders: nil,
			expectedErrCode:       ErrMissingSignHeadersTag,
		},
		// Test case - 3.
		// Test case with valid inputs.
		{
			inputSignElement:      "SignedHeaders=host;x-amz-content-sha256;x-amz-date",
			expectedSignedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
			expectedErrCode:       ErrNone,
		},
	}

	for i, testCase := range testCases {
		actualSignedHeaders, actualErrCode := parseSignedHeader(testCase.inputSignElement)
		if testCase.expectedErrCode != actualErrCode {
			t.Errorf("Test %d: Expected the APIErrCode to be %d, got %d", i+1, testCase.expectedErrCode, actualErrCode)
		}
		if actualErrCode == ErrNone {
			if strings.Join(testCase.expectedSignedHeaders, ",") != strings.Join(actualSignedHeaders, ",") {
				t.Errorf("Test %d: Expected the result to be \"%v\", but got \"%v\". ", i+1, testCase.expectedSignedHeaders, actualSignedHeaders)
			}
		}
	}
}

// TestParseSignV4 - Tests Parsing of v4 signature form the authorization string.
func TestParseSignV4(t *testing.T) {
	sampleTimeStr := UTCNow().Format(yyyymmdd)
	testCases := []struct {
		inputV4AuthStr    string
		expectedAuthField signValues
		expectedErrCode   APIErrorCode
	}{
		// Test case - 1.
		// Test case with empty auth string.
		{
			inputV4AuthStr:    "",
			expectedAuthField: signValues{},
			expectedErrCode:   ErrAuthHeaderEmpty,
		},
		// Test case - 2.
		// Test case with no sign v4 Algorithm prefix.
		// A valid authorization string should begin(prefix)
		{
			inputV4AuthStr:    "no-singv4AlgorithmPrefix",
			expectedAuthField: signValues{},
			expectedErrCode:   ErrSignatureVersionNotSupported,
		},
		// Test case - 3.
		// Test case with missing fields.
		// A valid authorization string should have 3 fields.
		{
			inputV4AuthStr:    signV4Algorithm,
			expectedAuthField: signValues{},
			expectedErrCode:   ErrMissingFields,
		},
		// Test case - 4.
		// Test case with invalid credential field.
		{
			inputV4AuthStr:    signV4Algorithm + " Cred=,a,b",
			expectedAuthField: signValues{},
			expectedErrCode:   ErrMissingCredTag,
		},
		// Test case - 5.
		// Auth field with missing "SigHeaderTag",ErrMissingSignHeadersTag expected.
		// A valid credential is generated.
		// Test case with invalid credential field.
		{
			inputV4AuthStr: signV4Algorithm +
				strings.Join([]string{
					// generating a valid credential field.
					generateCredentialStr(
						"Z7IXGOO6BZ0REAN1Q26I",
						sampleTimeStr,
						"us-west-1",
						"s3",
						"aws4_request"),
					// Incorrect SignedHeader field.
					"SignIncorrectHeader=",
					"b",
				}, ","),

			expectedAuthField: signValues{},
			expectedErrCode:   ErrMissingSignHeadersTag,
		},
		// Test case - 6.
		// Auth string with missing "SignatureTag",ErrMissingSignTag expected.
		// A valid credential is generated.
		// Test case with invalid credential field.
		{
			inputV4AuthStr: signV4Algorithm +
				strings.Join([]string{
					// generating a valid credential.
					generateCredentialStr(
						"Z7IXGOO6BZ0REAN1Q26I",
						sampleTimeStr,
						"us-west-1",
						"s3",
						"aws4_request"),
					// valid SignedHeader.
					"SignedHeaders=host;x-amz-content-sha256;x-amz-date",
					// invalid Signature field.
					// a valid signature is of form "Signature="
					"Sign=",
				}, ","),

			expectedAuthField: signValues{},
			expectedErrCode:   ErrMissingSignTag,
		},
		// Test case - 7.
		{
			inputV4AuthStr: signV4Algorithm +
				strings.Join([]string{
					// generating a valid credential.
					generateCredentialStr(
						"Z7IXGOO6BZ0REAN1Q26I",
						sampleTimeStr,
						"us-west-1",
						"s3",
						"aws4_request"),
					// valid SignedHeader.
					"SignedHeaders=host;x-amz-content-sha256;x-amz-date",
					// valid Signature field.
					// a valid signature is of form "Signature="
					"Signature=abcd",
				}, ","),
			expectedAuthField: signValues{
				Credential: generateCredentials(
					t,
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				SignedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
				Signature:     "abcd",
			},
			expectedErrCode: ErrNone,
		},
		// Test case - 8.
		{
			inputV4AuthStr: signV4Algorithm +
				strings.Join([]string{
					// generating a valid credential.
					generateCredentialStr(
						"access key",
						sampleTimeStr,
						"us-west-1",
						"s3",
						"aws4_request"),
					// valid SignedHeader.
					"SignedHeaders=host;x-amz-content-sha256;x-amz-date",
					// valid Signature field.
					// a valid signature is of form "Signature="
					"Signature=abcd",
				}, ","),
			expectedAuthField: signValues{
				Credential: generateCredentials(
					t,
					"access key",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				SignedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
				Signature:     "abcd",
			},
			expectedErrCode: ErrNone,
		},
	}

	for i, testCase := range testCases {
		parsedAuthField, actualErrCode := parseSignV4(testCase.inputV4AuthStr, "", "s3")

		if testCase.expectedErrCode != actualErrCode {
			t.Fatalf("Test %d: Expected the APIErrCode to be %d, got %d", i+1, testCase.expectedErrCode, actualErrCode)
		}

		if actualErrCode == ErrNone {
			// validating the extracted/parsed credential fields.
			validateCredentialfields(t, i+1, testCase.expectedAuthField.Credential, parsedAuthField.Credential)

			// validating the extraction/parsing of signature field.
			if !compareSignatureV4(testCase.expectedAuthField.Signature, parsedAuthField.Signature) {
				t.Errorf("Test %d: Parsed Signature field mismatch: Expected \"%s\", got \"%s\"", i+1, testCase.expectedAuthField.Signature, parsedAuthField.Signature)
			}

			// validating the extracted signed headers.
			if strings.Join(testCase.expectedAuthField.SignedHeaders, ",") != strings.Join(parsedAuthField.SignedHeaders, ",") {
				t.Errorf("Test %d: Expected the result to be \"%v\", but got \"%v\". ", i+1, testCase.expectedAuthField, parsedAuthField.SignedHeaders)
			}
		}
	}
}

// TestDoesV4PresignParamsExist - tests validate the logic to
func TestDoesV4PresignParamsExist(t *testing.T) {
	testCases := []struct {
		inputQueryKeyVals []string
		expectedErrCode   APIErrorCode
	}{
		// Test case - 1.
		// contains all query param keys which are necessary for v4 presign request.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedErrCode: ErrNone,
		},
		// Test case - 2.
		// missing 	"X-Amz-Algorithm" in tdhe query param.
		// contains all query param keys which are necessary for v4 presign request.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedErrCode: ErrInvalidQueryParams,
		},
		// Test case - 3.
		// missing "X-Amz-Credential" in the query param.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedErrCode: ErrInvalidQueryParams,
		},
		// Test case - 4.
		// missing "X-Amz-Signature" in the query param.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Credential", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedErrCode: ErrInvalidQueryParams,
		},
		// Test case - 5.
		// missing "X-Amz-Date" in the query param.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedErrCode: ErrInvalidQueryParams,
		},
		// Test case - 6.
		// missing "X-Amz-SignedHeaders" in the query param.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-Expires", "",
			},
			expectedErrCode: ErrInvalidQueryParams,
		},
		// Test case - 7.
		// missing "X-Amz-Expires" in the query param.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
			},
			expectedErrCode: ErrInvalidQueryParams,
		},
	}

	for i, testCase := range testCases {
		inputQuery := url.Values{}
		// iterating through input query key value and setting the inputQuery of type url.Values.
		for j := 0; j < len(testCase.inputQueryKeyVals)-1; j += 2 {
			inputQuery.Set(testCase.inputQueryKeyVals[j], testCase.inputQueryKeyVals[j+1])
		}

		actualErrCode := doesV4PresignParamsExist(inputQuery)

		if testCase.expectedErrCode != actualErrCode {
			t.Fatalf("Test %d: Expected the APIErrCode to be %d, got %d", i+1, testCase.expectedErrCode, actualErrCode)
		}
	}
}

// TestParsePreSignV4 - Validates the parsing logic of Presignied v4 request from its url query values.
func TestParsePreSignV4(t *testing.T) {
	// converts the duration in seconds into string format.
	getDurationStr := strconv.Itoa

	// used in expected preSignValues, preSignValues.Date is of type time.Time .
	queryTime := UTCNow()

	sampleTimeStr := UTCNow().Format(yyyymmdd)

	testCases := []struct {
		inputQueryKeyVals     []string
		expectedPreSignValues preSignValues
		expectedErrCode       APIErrorCode
	}{
		// Test case - 1.
		// A Valid v4 presign URL requires the following params to be in the query.
		// "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Signature", " X-Amz-Date", "X-Amz-SignedHeaders", "X-Amz-Expires".
		// If these params are missing its expected to get ErrInvalidQueryParams .
		// In the following test case 2 out of 6 query params are missing.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "",
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-Expires", "",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrInvalidQueryParams,
		},
		// Test case - 2.
		// Test case with invalid  "X-Amz-Algorithm" query value.
		// The other query params should exist, other wise ErrInvalidQueryParams will be returned because of missing fields.
		{
			inputQueryKeyVals: []string{
				"X-Amz-Algorithm", "InvalidValue",
				"X-Amz-Credential", "",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrInvalidQuerySignatureAlgo,
		},
		// Test case - 3.
		// Test case with valid "X-Amz-Algorithm" query value, but invalid  "X-Amz-Credential" header.
		// Malformed crenential.
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", "invalid-credential",
				"X-Amz-Signature", "",
				"X-Amz-Date", "",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrCredMalformed,
		},

		// Test case - 4.
		// Test case with valid "X-Amz-Algorithm" query value.
		// Malformed date.
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", joinWithSlash(
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				// invalid "X-Amz-Date" query.
				"X-Amz-Date", "invalid-time",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Expires", "",
				"X-Amz-Signature", "",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrMalformedPresignedDate,
		},
		// Test case - 5.
		// Test case with valid "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date" query value.
		// Malformed Expiry, a valid expiry should be of format "<int>s".
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", joinWithSlash(
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				// valid "X-Amz-Date" query.
				"X-Amz-Date", UTCNow().Format(iso8601Format),
				"X-Amz-Expires", "MalformedExpiry",
				"X-Amz-SignedHeaders", "",
				"X-Amz-Signature", "",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrMalformedExpires,
		},
		// Test case - 6.
		// Test case with negative X-Amz-Expires header.
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", joinWithSlash(
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				// valid "X-Amz-Date" query.
				"X-Amz-Date", queryTime.UTC().Format(iso8601Format),
				"X-Amz-Expires", getDurationStr(-1),
				"X-Amz-Signature", "abcd",
				"X-Amz-SignedHeaders", "host;x-amz-content-sha256;x-amz-date",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrNegativeExpires,
		},
		// Test case - 7.
		// Test case with empty X-Amz-SignedHeaders.
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", joinWithSlash(
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				// valid "X-Amz-Date" query.
				"X-Amz-Date", queryTime.UTC().Format(iso8601Format),
				"X-Amz-Expires", getDurationStr(100),
				"X-Amz-Signature", "abcd",
				"X-Amz-SignedHeaders", "",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrMissingFields,
		},
		// Test case - 8.
		// Test case with valid "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date" query value.
		// Malformed Expiry, a valid expiry should be of format "<int>s".
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", joinWithSlash(
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				// valid "X-Amz-Date" query.
				"X-Amz-Date", queryTime.UTC().Format(iso8601Format),
				"X-Amz-Expires", getDurationStr(100),
				"X-Amz-Signature", "abcd",
				"X-Amz-SignedHeaders", "host;x-amz-content-sha256;x-amz-date",
			},
			expectedPreSignValues: preSignValues{
				signValues{
					// Credentials.
					generateCredentials(
						t,
						"Z7IXGOO6BZ0REAN1Q26I",
						sampleTimeStr,
						"us-west-1",
						"s3",
						"aws4_request",
					),
					// SignedHeaders.
					[]string{"host", "x-amz-content-sha256", "x-amz-date"},
					// Signature.
					"abcd",
				},
				// Date
				queryTime,
				// Expires.
				100 * time.Second,
			},
			expectedErrCode: ErrNone,
		},

		// Test case - 9.
		// Test case with value greater than 604800 in X-Amz-Expires header.
		{
			inputQueryKeyVals: []string{
				// valid  "X-Amz-Algorithm" header.
				"X-Amz-Algorithm", signV4Algorithm,
				// valid  "X-Amz-Credential" header.
				"X-Amz-Credential", joinWithSlash(
					"Z7IXGOO6BZ0REAN1Q26I",
					sampleTimeStr,
					"us-west-1",
					"s3",
					"aws4_request"),
				// valid "X-Amz-Date" query.
				"X-Amz-Date", queryTime.UTC().Format(iso8601Format),
				// Invalid Expiry time greater than 7 days (604800 in seconds).
				"X-Amz-Expires", getDurationStr(605000),
				"X-Amz-Signature", "abcd",
				"X-Amz-SignedHeaders", "host;x-amz-content-sha256;x-amz-date",
			},
			expectedPreSignValues: preSignValues{},
			expectedErrCode:       ErrMaximumExpires,
		},
	}

	for i, testCase := range testCases {
		inputQuery := url.Values{}
		// iterating through input query key value and setting the inputQuery of type url.Values.
		for j := 0; j < len(testCase.inputQueryKeyVals)-1; j += 2 {
			inputQuery.Set(testCase.inputQueryKeyVals[j], testCase.inputQueryKeyVals[j+1])
		}
		// call the function under test.
		parsedPreSign, actualErrCode := parsePreSignV4(inputQuery, "", serviceS3)
		if testCase.expectedErrCode != actualErrCode {
			t.Fatalf("Test %d: Expected the APIErrCode to be %d, got %d", i+1, testCase.expectedErrCode, actualErrCode)
		}
		if actualErrCode == ErrNone {
			// validating credentials.
			validateCredentialfields(t, i+1, testCase.expectedPreSignValues.Credential, parsedPreSign.Credential)
			// validating signed headers.
			if strings.Join(testCase.expectedPreSignValues.SignedHeaders, ",") != strings.Join(parsedPreSign.SignedHeaders, ",") {
				t.Errorf("Test %d: Expected the result to be \"%v\", but got \"%v\". ", i+1, testCase.expectedPreSignValues.SignedHeaders, parsedPreSign.SignedHeaders)
			}
			// validating signature field.
			if !compareSignatureV4(testCase.expectedPreSignValues.Signature, parsedPreSign.Signature) {
				t.Errorf("Test %d: Signature field mismatch: Expected \"%s\", got \"%s\"", i+1, testCase.expectedPreSignValues.Signature, parsedPreSign.Signature)
			}
			// validating expiry duration.
			if testCase.expectedPreSignValues.Expires != parsedPreSign.Expires {
				t.Errorf("Test %d: Expected expiry time to be %v, but got %v", i+1, testCase.expectedPreSignValues.Expires, parsedPreSign.Expires)
			}
			// validating presign date field.
			if testCase.expectedPreSignValues.Date.UTC().Format(iso8601Format) != parsedPreSign.Date.UTC().Format(iso8601Format) {
				t.Errorf("Test %d: Expected date to be %v, but got %v", i+1, testCase.expectedPreSignValues.Date.UTC().Format(iso8601Format), parsedPreSign.Date.UTC().Format(iso8601Format))
			}
		}
	}
}
