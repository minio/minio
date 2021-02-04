/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	"context"
)

// GetTotalCapacity gets the total capacity in the cluster.
func GetTotalCapacity(ctx context.Context) (capacity uint64) {
	d := globalNotificationSys.DiskHwInfo(ctx)
	for _, s := range d {
		capacity += s.GetTotalCapacity()
	}
	return
}

// GetTotalUsableCapacity gets the total usable capacity in the cluster.
// This value is not an accurate representation of total usable in a multi-tenant deployment.
func GetTotalUsableCapacity(ctx context.Context, s StorageInfo) (capacity float64) {
	raw := GetTotalCapacity(ctx)
	var approxDataBlocks float64
	var actualDisks float64
	for _, scData := range s.Backend.StandardSCData {
		approxDataBlocks += float64(scData)
		actualDisks += float64(scData + s.Backend.StandardSCParity)
	}
	ratio := approxDataBlocks / actualDisks
	return float64(raw) * ratio
}

// GetTotalCapacityFree gets the total capacity free in the cluster.
func GetTotalCapacityFree(ctx context.Context) (capacity uint64) {
	d := globalNotificationSys.DiskHwInfo(ctx)
	for _, s := range d {
		capacity += s.GetTotalFreeCapacity()
	}
	return
}

// GetTotalUsableCapacityFree gets the total usable capacity free in the cluster.
// This value is not an accurate representation of total free in a multi-tenant deployment.
func GetTotalUsableCapacityFree(ctx context.Context, s StorageInfo) (capacity float64) {
	raw := GetTotalCapacityFree(ctx)
	var approxDataBlocks float64
	var actualDisks float64
	for _, scData := range s.Backend.StandardSCData {
		approxDataBlocks += float64(scData)
		actualDisks += float64(scData + s.Backend.StandardSCParity)
	}
	ratio := approxDataBlocks / actualDisks
	return float64(raw) * ratio
}
