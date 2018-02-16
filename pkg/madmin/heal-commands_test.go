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
 *
 */

package madmin

import (
	"testing"
)

// Tests heal drives missing and offline counts.
func TestHealDriveCounts(t *testing.T) {
	rs := HealResultItem{}
	rs.Before.Drives = make([]HealDriveInfo, 20)
	rs.After.Drives = make([]HealDriveInfo, 20)
	for i := range rs.Before.Drives {
		if i < 4 {
			rs.Before.Drives[i] = HealDriveInfo{State: DriveStateMissing}
			rs.After.Drives[i] = HealDriveInfo{State: DriveStateMissing}
		} else if i > 4 && i < 15 {
			rs.Before.Drives[i] = HealDriveInfo{State: DriveStateOffline}
			rs.After.Drives[i] = HealDriveInfo{State: DriveStateOffline}
		} else if i > 15 {
			rs.Before.Drives[i] = HealDriveInfo{State: DriveStateCorrupt}
			rs.After.Drives[i] = HealDriveInfo{State: DriveStateCorrupt}
		} else {
			rs.Before.Drives[i] = HealDriveInfo{State: DriveStateOk}
			rs.After.Drives[i] = HealDriveInfo{State: DriveStateOk}
		}
	}

	i, j := rs.GetOnlineCounts()
	if i > 2 {
		t.Errorf("Expected '2', got %d before online disks", i)
	}
	if j > 2 {
		t.Errorf("Expected '2', got %d after online disks", j)
	}
	i, j = rs.GetOfflineCounts()
	if i > 10 {
		t.Errorf("Expected '10', got %d before offline disks", i)
	}
	if j > 10 {
		t.Errorf("Expected '10', got %d after offline disks", j)
	}
	i, j = rs.GetCorruptedCounts()
	if i > 4 {
		t.Errorf("Expected '4', got %d before corrupted disks", i)
	}
	if j > 4 {
		t.Errorf("Expected '4', got %d after corrupted disks", j)
	}
	i, j = rs.GetMissingCounts()
	if i > 4 {
		t.Errorf("Expected '4', got %d before missing disks", i)
	}
	if j > 4 {
		t.Errorf("Expected '4', got %d after missing disks", i)
	}
}
