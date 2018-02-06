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

package cmd

import (
	"os"
	"testing"
)

func (action InitActions) String() string {
	switch action {
	case InitObjectLayer:
		return "InitObjectLayer"
	case FormatDisks:
		return "FormatDisks"
	case WaitForFormatting:
		return "WaitForFormatting"
	case SuggestToHeal:
		return "SuggestToHeal"
	case WaitForAll:
		return "WaitForAll"
	case WaitForQuorum:
		return "WaitForQuorum"
	case WaitForConfig:
		return "WaitForConfig"
	case Abort:
		return "Abort"
	default:
		return "Unknown"
	}
}

func TestReduceInitXLErrs(t *testing.T) {
	_, fsDirs, err := prepareXL(4)
	if err != nil {
		t.Fatalf("Unable to initialize 'XL' object layer.")
	}

	// Remove all dirs.
	for _, dir := range fsDirs {
		defer os.RemoveAll(dir)
	}

	storageDisks, err := initStorageDisks(mustGetNewEndpointList(fsDirs...))
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	testCases := []struct {
		sErrs       []error
		expectedErr string
	}{
		{[]error{nil, nil, nil, nil}, ""},
		{[]error{errUnformattedDisk, nil, nil, nil}, "\n[01/04] " + storageDisks[0].String() + " : unformatted disk found"},
		{[]error{errUnformattedDisk, errUnformattedDisk, nil, nil}, "\n[01/04] " + storageDisks[0].String() + " : unformatted disk found" + "\n[02/04] " + storageDisks[1].String() + " : unformatted disk found"},
		{[]error{errUnformattedDisk, errUnformattedDisk, errRPCAPIVersionUnsupported, nil}, storageDisks[2].String() + ": Unsupported rpc API version"},
	}
	for i, test := range testCases {
		actual := reduceInitXLErrs(storageDisks, test.sErrs)
		if test.expectedErr == "" && actual != nil {
			t.Errorf("Test %d expected no error but received `%s`", i+1, actual.Error())
		}
		if test.expectedErr != "" && actual.Error() != test.expectedErr {
			t.Errorf("Test %d expected `%s` but received `%s`", i+1, test.expectedErr, actual.Error())
		}
	}
}

func TestPrepForInitXL(t *testing.T) {
	// All disks are unformatted, a fresh setup.
	allUnformatted := []error{
		errUnformattedDisk, errUnformattedDisk, errUnformattedDisk, errUnformattedDisk,
		errUnformattedDisk, errUnformattedDisk, errUnformattedDisk, errUnformattedDisk,
	}
	// All disks are formatted, possible restart of a node in a formatted setup.
	allFormatted := []error{
		nil, nil, nil, nil,
		nil, nil, nil, nil,
	}
	// Quorum number of disks are formatted and rest are offline.
	quorumFormatted := []error{
		nil, nil, nil, nil,
		nil, errDiskNotFound, errDiskNotFound, errDiskNotFound,
	}
	// Minority disks are corrupted, can be healed.
	minorityCorrupted := []error{
		errCorruptedFormat, errCorruptedFormat, errCorruptedFormat, nil,
		nil, nil, nil, nil,
	}
	// Majority disks are corrupted, pretty bad setup.
	majorityCorrupted := []error{
		errCorruptedFormat, errCorruptedFormat, errCorruptedFormat, errCorruptedFormat,
		errCorruptedFormat, nil, nil, nil,
	}
	// Quorum disks are unformatted, remaining yet to come online.
	quorumUnformatted := []error{
		errUnformattedDisk, errUnformattedDisk, errUnformattedDisk, errUnformattedDisk,
		errUnformattedDisk, errDiskNotFound, errDiskNotFound, errDiskNotFound,
	}
	quorumUnformattedSomeCorrupted := []error{
		errUnformattedDisk, errUnformattedDisk, errUnformattedDisk, errUnformattedDisk,
		errUnformattedDisk, errCorruptedFormat, errCorruptedFormat, errDiskNotFound,
	}

	// Quorum number of disks not online yet.
	noQuourm := []error{
		errDiskNotFound, errDiskNotFound, errDiskNotFound, errDiskNotFound,
		errDiskNotFound, nil, nil, nil,
	}
	// Invalid access key id.
	accessKeyIDErr := []error{
		errInvalidAccessKeyID, errInvalidAccessKeyID, errInvalidAccessKeyID, errInvalidAccessKeyID,
		errInvalidAccessKeyID, nil, nil, nil,
	}
	// Authentication error.
	authenticationErr := []error{
		nil, nil, nil, errAuthentication,
		errAuthentication, errAuthentication, errAuthentication, errAuthentication,
	}
	// Unsupported rpc API version.
	rpcUnsupportedVersion := []error{
		errRPCAPIVersionUnsupported, errRPCAPIVersionUnsupported, errRPCAPIVersionUnsupported, errRPCAPIVersionUnsupported,
		errRPCAPIVersionUnsupported, nil, nil, nil,
	}
	// Server time mismatch.
	serverTimeMismatch := []error{
		errServerTimeMismatch, errServerTimeMismatch, errServerTimeMismatch, errServerTimeMismatch,
		errServerTimeMismatch, nil, nil, nil,
	}
	// Collection of config errs.
	configErrs := []error{
		errServerTimeMismatch, errServerTimeMismatch, errRPCAPIVersionUnsupported, errAuthentication,
		errInvalidAccessKeyID, nil, nil, nil,
	}
	// Suggest to heal under formatted disks in quorum.
	formattedDisksInQuorum := []error{
		nil, nil, nil, nil,
		errUnformattedDisk, errUnformattedDisk, errDiskNotFound, errDiskNotFound,
	}
	// Wait for all under undecisive state.
	undecisiveErrs1 := []error{
		errDiskNotFound, nil, nil, nil,
		errUnformattedDisk, errUnformattedDisk, errDiskNotFound, errDiskNotFound,
	}
	undecisiveErrs2 := []error{
		errDiskNotFound, errDiskNotFound, errDiskNotFound, errDiskNotFound,
		errUnformattedDisk, errUnformattedDisk, errUnformattedDisk, errUnformattedDisk,
	}

	testCases := []struct {
		// Params for prepForInit().
		firstDisk bool
		errs      []error
		diskCount int
		action    InitActions
	}{
		// Local disks.
		{true, allFormatted, 8, InitObjectLayer},
		{true, quorumFormatted, 8, InitObjectLayer},
		{true, allUnformatted, 8, FormatDisks},
		{true, quorumUnformatted, 8, WaitForAll},
		{true, quorumUnformattedSomeCorrupted, 8, Abort},
		{true, noQuourm, 8, WaitForQuorum},
		{true, minorityCorrupted, 8, SuggestToHeal},
		{true, majorityCorrupted, 8, Abort},
		// Remote disks.
		{false, allFormatted, 8, InitObjectLayer},
		{false, quorumFormatted, 8, InitObjectLayer},
		{false, allUnformatted, 8, WaitForFormatting},
		{false, quorumUnformatted, 8, WaitForAll},
		{false, quorumUnformattedSomeCorrupted, 8, Abort},
		{false, noQuourm, 8, WaitForQuorum},
		{false, minorityCorrupted, 8, SuggestToHeal},
		{false, formattedDisksInQuorum, 8, SuggestToHeal},
		{false, majorityCorrupted, 8, Abort},
		{false, undecisiveErrs1, 8, WaitForAll},
		{false, undecisiveErrs2, 8, WaitForAll},
		// Config mistakes.
		{true, accessKeyIDErr, 8, WaitForConfig},
		{true, authenticationErr, 8, WaitForConfig},
		{true, rpcUnsupportedVersion, 8, WaitForConfig},
		{true, serverTimeMismatch, 8, WaitForConfig},
		{true, configErrs, 8, WaitForConfig},
	}
	for i, test := range testCases {
		actual := prepForInitXL(test.firstDisk, test.errs, test.diskCount)
		if actual != test.action {
			t.Errorf("Test %d expected %s but received %s\n", i+1, test.action, actual)
		}
	}
}
