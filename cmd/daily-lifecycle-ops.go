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
	"github.com/minio/minio/pkg/lifecycle"
)

const (
	bgLifecycleInterval = 24 * time.Hour
	bgLifecycleTick     = time.Hour
)

type lifecycleOps struct {
	LastActivity time.Time
}

// Register to the daily objects listing
var globalLifecycleOps = &lifecycleOps{}

func getLocalBgLifecycleOpsStatus() BgLifecycleOpsStatus {
	return BgLifecycleOpsStatus{
		LastActivity: globalLifecycleOps.LastActivity,
	}
}

// initDailyLifecycle starts the routine that receives the daily
// listing of all objects and applies any matching bucket lifecycle
// rules.
func initDailyLifecycle() {
	go startDailyLifecycle()
}

func startDailyLifecycle() {
	var objAPI ObjectLayer
	var ctx = context.Background()

	// Wait until the object API is ready
	for {
		objAPI = newObjectLayerFn()
		if objAPI == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	// Calculate the time of the last lifecycle operation in all peers node of the cluster
	computeLastLifecycleActivity := func(status []BgOpsStatus) time.Time {
		var lastAct time.Time
		for _, st := range status {
			if st.LifecycleOps.LastActivity.After(lastAct) {
				lastAct = st.LifecycleOps.LastActivity
			}
		}
		return lastAct
	}

	for {
		// Check if we should perform lifecycle ops based on the last lifecycle activity, sleep one hour otherwise
		allLifecycleStatus := []BgOpsStatus{
			{LifecycleOps: getLocalBgLifecycleOpsStatus()},
		}
		if globalIsDistXL {
			allLifecycleStatus = append(allLifecycleStatus, globalNotificationSys.BackgroundOpsStatus()...)
		}
		lastAct := computeLastLifecycleActivity(allLifecycleStatus)
		if !lastAct.IsZero() && time.Since(lastAct) < bgLifecycleInterval {
			time.Sleep(bgLifecycleTick)
		}

		// Perform one lifecycle operation
		err := lifecycleRound(ctx, objAPI)
		switch err.(type) {
		// Unable to hold a lock means there is another
		// instance doing the lifecycle round round
		case OperationTimedOut:
			time.Sleep(bgLifecycleTick)
		default:
			logger.LogIf(ctx, err)
			time.Sleep(time.Minute)
			continue
		}

	}
}

var lifecycleTimeout = newDynamicTimeout(60*time.Second, time.Second)

func lifecycleRound(ctx context.Context, objAPI ObjectLayer) error {
	// Lock to avoid concurrent lifecycle ops from other nodes
	sweepLock := globalNSMutex.NewNSLock(ctx, "system", "daily-lifecycle-ops")
	if err := sweepLock.GetLock(lifecycleTimeout); err != nil {
		return err
	}
	defer sweepLock.Unlock()

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

		// Calculate the common prefix of all lifecycle rules
		var prefixes []string
		for _, rule := range l.Rules {
			prefixes = append(prefixes, rule.Filter.Prefix)
		}
		commonPrefix := lcp(prefixes)

		// List all objects and calculate lifecycle action based on object name & object modtime
		marker := ""
		for {
			res, err := objAPI.ListObjects(ctx, bucket.Name, commonPrefix, marker, "", 1000)
			if err != nil {
				continue
			}
			var objects []string
			for _, obj := range res.Objects {
				// Find the action that need to be executed
				action := l.ComputeAction(obj.Name, obj.ModTime)
				switch action {
				case lifecycle.DeleteAction:
					objects = append(objects, obj.Name)
				default:
					// Do nothing, for now.
				}
			}
			// Deletes a list of objects.
			objAPI.DeleteObjects(ctx, bucket.Name, objects)
			if !res.IsTruncated {
				// We are done here, proceed to next bucket.
				break
			}
			marker = res.NextMarker
		}
	}

	return nil
}
