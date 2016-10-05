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

import "testing"

func (action InitActions) String() string {
	switch action {
	case InitObjectLayer:
		return "InitObjectLayer"
	case FormatDisks:
		return "FormatDisks"
	case WaitForFormatting:
		return "WaitForFormatting"
	case WaitForHeal:
		return "WaitForHeal"
	case WaitForAll:
		return "WaitForAll"
	case WaitForQuorum:
		return "WaitForQuorum"
	case Abort:
		return "Abort"
	default:
		return "Unknown"
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
		{true, minorityCorrupted, 8, WaitForHeal},
		{true, majorityCorrupted, 8, Abort},
		// Remote disks.
		{false, allFormatted, 8, InitObjectLayer},
		{false, quorumFormatted, 8, InitObjectLayer},
		{false, allUnformatted, 8, WaitForFormatting},
		{false, quorumUnformatted, 8, WaitForAll},
		{false, quorumUnformattedSomeCorrupted, 8, Abort},
		{false, noQuourm, 8, WaitForQuorum},
		{false, minorityCorrupted, 8, WaitForHeal},
		{false, majorityCorrupted, 8, Abort},
	}
	for i, test := range testCases {
		actual := prepForInitXL(test.firstDisk, test.errs, test.diskCount)
		if actual != test.action {
			t.Errorf("Test %d expected %s but receieved %s\n", i+1, test.action, actual)
		}
	}
}
