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
	"runtime"
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
func TestPrepForInit(t *testing.T) {
	var disks []string
	if runtime.GOOS == "windows" {
		disks = []string{
			`c:\mnt\disk1`,
			`c:\mnt\disk2`,
			`c:\mnt\disk3`,
			`c:\mnt\disk4`,
			`c:\mnt\disk5`,
			`c:\mnt\disk6`,
			`c:\mnt\disk7`,
			`c:\mnt\disk8`,
		}
	} else {
		disks = []string{
			"/mnt/disk1",
			"/mnt/disk2",
			"/mnt/disk3",
			"/mnt/disk4",
			"/mnt/disk5",
			"/mnt/disk6",
			"/mnt/disk7",
			"/mnt/disk8",
		}
	}
	// Building up disks that resolve to localhost and remote w.r.t isLocalStorage().
	var (
		disksLocal  []string
		disksRemote []string
	)
	for i := range disks {
		disksLocal = append(disksLocal, "localhost:"+disks[i])
	}
	// Using 4.4.4.4 as a known non-local address.
	for i := range disks {
		disksRemote = append(disksRemote, "4.4.4.4:"+disks[i])
	}
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
		disks     []string
		errs      []error
		diskCount int
		action    InitActions
	}{
		// Local disks.
		{disksLocal, allFormatted, 8, InitObjectLayer},
		{disksLocal, quorumFormatted, 8, InitObjectLayer},
		{disksLocal, allUnformatted, 8, FormatDisks},
		{disksLocal, quorumUnformatted, 8, WaitForAll},
		{disksLocal, quorumUnformattedSomeCorrupted, 8, WaitForHeal},
		{disksLocal, noQuourm, 8, WaitForQuorum},
		{disksLocal, minorityCorrupted, 8, WaitForHeal},
		{disksLocal, majorityCorrupted, 8, Abort},
		// Remote disks.
		{disksRemote, allFormatted, 8, InitObjectLayer},
		{disksRemote, quorumFormatted, 8, InitObjectLayer},
		{disksRemote, allUnformatted, 8, WaitForFormatting},
		{disksRemote, quorumUnformatted, 8, WaitForAll},
		{disksRemote, quorumUnformattedSomeCorrupted, 8, WaitForHeal},
		{disksRemote, noQuourm, 8, WaitForQuorum},
		{disksRemote, minorityCorrupted, 8, WaitForHeal},
		{disksRemote, majorityCorrupted, 8, Abort},
	}
	for i, test := range testCases {
		actual := prepForInit(test.disks, test.errs, test.diskCount)
		if actual != test.action {
			t.Errorf("Test %d expected %s but receieved %s\n", i+1, test.action, actual)
		}
	}
}
