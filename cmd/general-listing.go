/*
 * MinIO Cloud Storage, (C) 2015-2019 MinIO, Inc.
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
)

const (
	bgGeneralListingEachCycles = 5
	bgGeneralListingInterval   = 5 * time.Second
	bgGeneralListingTick       = time.Second
)

var globalGenListingListeners = []func(ObjectInfo, bool) error{}

func initGeneralListing(ctx context.Context, objAPI ObjectLayer) {
	go startGeneralListing(ctx, objAPI)
}

func startGeneralListing(ctx context.Context, objAPI ObjectLayer) {
	cycles := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.NewTimer(bgGeneralListingInterval).C:
			cycles += 1
			cycles %= bgGeneralListingEachCycles
			// Perform one lifecycle operation
			logger.LogIf(ctx, generalListingRound(ctx, objAPI, cycles == 0))
		}
	}
}

func generalListingRound(ctx context.Context, objAPI ObjectLayer, listWithMetadata bool) error {
	buckets, err := objAPI.ListBuckets(ctx)
	if err != nil {
		return err
	}

	results := make(chan ObjectInfo)
	go func() {
		for r := range results {
			for _, fn := range globalGenListingListeners {
				fn(r, listWithMetadata)
			}
		}
	}()

	for _, bucket := range buckets {
		err := objAPI.Walk(ctx, bucket.Name, "", listWithMetadata, results)
		logger.LogIf(ctx, err)
	}

	return nil
}
