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

func GetTotalCapacity(ctx context.Context) (capacity uint64) {
	d := globalNotificationSys.DiskHwInfo(ctx)
	for _, s := range d {
		capacity += s.GetTotalCapacity()
	}
	return
}

func GetTotalUsableCapacity(ctx context.Context, s StorageInfo) (capacity float64) {
	raw := GetTotalCapacity(ctx)
	ratio := float64(s.Backend.StandardSCData) / float64(s.Backend.StandardSCData+s.Backend.StandardSCParity)
	return float64(raw) * ratio
}

func GetTotalCapacityFree(ctx context.Context) (capacity uint64) {
	d := globalNotificationSys.DiskHwInfo(ctx)
	for _, s := range d {
		capacity += s.GetTotalFreeCapacity()
	}
	return
}

func GetTotalUsableCapacityFree(ctx context.Context, s StorageInfo) (capacity float64) {
	raw := GetTotalCapacityFree(ctx)
	ratio := float64(s.Backend.StandardSCData) / float64(s.Backend.StandardSCData+s.Backend.StandardSCParity)
	return float64(raw) * ratio
}
