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
 */

package cmd

import (
	"context"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/event"
)

func init() {
	globalGenListingListeners = append(globalGenListingListeners, continousLifecycle)
}

func continousLifecycle(objInfo ObjectInfo, withMetadata bool) error {
	if !withMetadata {
		return nil
	}

	// Check if the current bucket has a configured lifecycle policy, skip otherwise
	l, ok := globalLifecycleSys.Get(objInfo.Bucket)
	if !ok {
		return nil
	}

	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		return nil
	}

	ctx := context.Background()

	if l.ComputeAction(objInfo.Name, objInfo.UserTags, objInfo.ModTime) == lifecycle.DeleteAction {
		err := objAPI.DeleteObject(ctx, objInfo.Bucket, objInfo.Name)
		if err != nil {
			logger.LogIf(ctx, err)
		} else {
			// Notify object deleted event.
			sendEvent(eventArgs{
				EventName:  event.ObjectRemovedDelete,
				BucketName: objInfo.Bucket,
				Object:     ObjectInfo{Name: objInfo.Name},
				Host:       "Internal: [ILM-EXPIRY]",
			})
		}
	}

	return nil
}
