/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"reflect"
	"testing"
)

// Sort valid disks info.
func TestSortingValidDisks(t *testing.T) {
	testCases := []struct {
		disksInfo      []DiskInfo
		validDisksInfo []DiskInfo
	}{
		// One of the disks is offline.
		{
			disksInfo: []DiskInfo{
				{Total: 150, Free: 10},
				{Total: 0, Free: 0},
				{Total: 200, Free: 10},
				{Total: 100, Free: 10},
			},
			validDisksInfo: []DiskInfo{
				{Total: 100, Free: 10},
				{Total: 150, Free: 10},
				{Total: 200, Free: 10},
			},
		},
		// All disks are online.
		{
			disksInfo: []DiskInfo{
				{Total: 150, Free: 10},
				{Total: 200, Free: 10},
				{Total: 100, Free: 10},
				{Total: 115, Free: 10},
			},
			validDisksInfo: []DiskInfo{
				{Total: 100, Free: 10},
				{Total: 115, Free: 10},
				{Total: 150, Free: 10},
				{Total: 200, Free: 10},
			},
		},
	}

	for i, testCase := range testCases {
		validDisksInfo := sortValidDisksInfo(testCase.disksInfo)
		if !reflect.DeepEqual(validDisksInfo, testCase.validDisksInfo) {
			t.Errorf("Test %d: Expected %#v, Got %#v", i+1, testCase.validDisksInfo, validDisksInfo)
		}
	}
}
