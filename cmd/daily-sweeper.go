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
	"math/rand"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// The list of modules listening for the daily listing of all objects
// such as the daily heal ops, disk usage and bucket lifecycle management.
var globalDailySweepListeners = make([]chan string, 0)
var globalDailySweepListenersMu = sync.Mutex{}

// Add a new listener to the daily objects listing
func registerDailySweepListener(ch chan string) {
	globalDailySweepListenersMu.Lock()
	defer globalDailySweepListenersMu.Unlock()

	globalDailySweepListeners = append(globalDailySweepListeners, ch)
}

// Safe copy of globalDailySweepListeners content
func copyDailySweepListeners() []chan string {
	globalDailySweepListenersMu.Lock()
	defer globalDailySweepListenersMu.Unlock()

	var listenersCopy = make([]chan string, len(globalDailySweepListeners))
	copy(listenersCopy, globalDailySweepListeners)

	return listenersCopy
}

var sweepTimeout = newDynamicTimeout(60*time.Second, time.Second)

// sweepRound will list all objects, having read quorum or not and
// feeds to all listeners, such as the background healing
func sweepRound(ctx context.Context, objAPI ObjectLayer) error {
	// General lock so we avoid parallel daily sweep by different instances.
	sweepLock := globalNSMutex.NewNSLock(ctx, "system", "daily-sweep")
	if err := sweepLock.GetLock(sweepTimeout); err != nil {
		return err
	}
	defer sweepLock.Unlock()

	buckets, err := objAPI.ListBuckets(ctx)
	if err != nil {
		return err
	}

	// List all objects, having read quorum or not in all buckets
	// and send them to all the registered sweep listeners
	for _, bucket := range buckets {
		// Send bucket names to all listeners
		for _, l := range copyDailySweepListeners() {
			l <- bucket.Name
		}

		marker := ""
		for {
			if globalHTTPServer != nil {
				// Wait at max 10 minute for an inprogress request before proceeding to heal
				waitCount := 600
				// Any requests in progress, delay the heal.
				for (globalHTTPServer.GetRequestCount() >= int32(globalXLSetCount*globalXLSetDriveCount)) &&
					waitCount > 0 {
					waitCount--
					time.Sleep(1 * time.Second)
				}
			}

			res, err := objAPI.ListObjectsHeal(ctx, bucket.Name, "", marker, "", 1000)
			if err != nil {
				continue
			}

			for _, obj := range res.Objects {
				for _, l := range copyDailySweepListeners() {
					l <- pathJoin(bucket.Name, obj.Name)
				}
			}
			if !res.IsTruncated {
				break
			} else {
				marker = res.NextMarker
			}
		}
	}

	return nil
}

// initDailySweeper creates a go-routine which will list all
// objects in all buckets in a daily basis
func initDailySweeper() {
	go dailySweeper()
}

// List all objects in all buckets in a daily basis
func dailySweeper() {
	var lastSweepTime time.Time
	var objAPI ObjectLayer

	var ctx = context.Background()

	// Wait until the object layer is ready
	for {
		objAPI = newObjectLayerFn()
		if objAPI == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	// Start with random sleep time, so as to avoid "synchronous checks" between servers
	time.Sleep(time.Duration(rand.Float64() * float64(time.Hour)))

	for {
		if time.Since(lastSweepTime) < 30*24*time.Hour {
			time.Sleep(time.Hour)
			continue
		}

		err := sweepRound(ctx, objAPI)
		if err != nil {
			switch err.(type) {
			// Unable to hold a lock means there is another
			// instance doing the sweep round
			case OperationTimedOut:
				lastSweepTime = time.Now()
				continue
			}
			logger.LogIf(ctx, err)
			time.Sleep(time.Minute)
			continue
		}
		lastSweepTime = time.Now()
	}
}
