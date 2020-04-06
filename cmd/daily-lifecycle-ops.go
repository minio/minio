/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/event"
)

const (
	bgLifecycleInterval = 24 * time.Hour
	bgLifecycleTick     = time.Hour
)

// initDailyLifecycle starts the routine that receives the daily
// listing of all objects and applies any matching bucket lifecycle
// rules.
func initDailyLifecycle(ctx context.Context, objAPI ObjectLayer) {
	go startDailyLifecycle(ctx, objAPI)
}

func startDailyLifecycle(ctx context.Context, objAPI ObjectLayer) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.NewTimer(bgLifecycleInterval).C:
			// Perform one lifecycle operation
			logger.LogIf(ctx, lifecycleRound(ctx, objAPI))
		}

	}
}

func lifecycleRound(ctx context.Context, objAPI ObjectLayer) error {
	// No action is expected when WORM is enabled
	if globalWORMEnabled {
		return nil
	}

	buckets, err := objAPI.ListBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		// Check if the current bucket has a configured lifecycle policy, skip otherwise
		l, ok := globalLifecycleSys.Get(bucket.Name)
		if !ok {
			continue
		}

		_, bucketHasLockConfig := globalBucketObjectLockConfig.Get(bucket.Name)

		// Calculate the common prefix of all lifecycle rules
		var prefixes []string
		for _, rule := range l.Rules {
			prefixes = append(prefixes, rule.Prefix())
		}
		commonPrefix := lcp(prefixes)

		// Allocate new results channel to receive ObjectInfo.
		objInfoCh := make(chan ObjectInfo)

		// Walk through all objects
		if err := objAPI.Walk(ctx, bucket.Name, commonPrefix, objInfoCh); err != nil {
			return err
		}

		for {
			var objects []string
			for obj := range objInfoCh {
				if len(objects) == maxObjectList {
					// Reached maximum delete requests, attempt a delete for now.
					break
				}
				// Find the action that need to be executed
				if l.ComputeAction(obj.Name, obj.UserTags, obj.ModTime) == lifecycle.DeleteAction {
					if bucketHasLockConfig && enforceRetentionForLifecycle(ctx, obj) {
						continue
					}
					objects = append(objects, obj.Name)
				}
			}

			// Nothing to do.
			if len(objects) == 0 {
				break
			}

			waitForLowHTTPReq(int32(globalEndpoints.NEndpoints()))

			// Deletes a list of objects.
			deleteErrs, err := objAPI.DeleteObjects(ctx, bucket.Name, objects)
			if err != nil {
				logger.LogIf(ctx, err)
			} else {
				for i := range deleteErrs {
					if deleteErrs[i] != nil {
						logger.LogIf(ctx, deleteErrs[i])
						continue
					}
					// Notify object deleted event.
					sendEvent(eventArgs{
						EventName:  event.ObjectRemovedDelete,
						BucketName: bucket.Name,
						Object: ObjectInfo{
							Name: objects[i],
						},
						Host: "Internal: [ILM-EXPIRY]",
					})
				}
			}
		}
	}

	return nil
}
