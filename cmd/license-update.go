// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/minio/minio/internal/logger"
	"github.com/tidwall/gjson"
)

const (
	licUpdateCycle = 24 * time.Hour * 30
	licRenewPath   = "/api/cluster/renew-license"
)

// initlicenseUpdateJob start the periodic license update job in the background.
func initLicenseUpdateJob(ctx context.Context, objAPI ObjectLayer) {
	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Leader node (that successfully acquires the lock inside licenceUpdaterLoop)
		// will keep performing the license update. If the leader goes down for some
		// reason, the lock will be released and another node will acquire it and
		// take over because of this loop.
		for {
			licenceUpdaterLoop(ctx, objAPI)

			// license update stopped for some reason.
			// sleep for some time and try again.
			duration := time.Duration(r.Float64() * float64(time.Hour))
			if duration < time.Second {
				// Make sure to sleep at least a second to avoid high CPU ticks.
				duration = time.Second
			}
			time.Sleep(duration)
		}
	}()
}

func licenceUpdaterLoop(ctx context.Context, objAPI ObjectLayer) {
	ctx, cancel := globalLeaderLock.GetLock(ctx)
	defer cancel()

	licenseUpdateTimer := time.NewTimer(licUpdateCycle)
	defer licenseUpdateTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-licenseUpdateTimer.C:

			if globalSubnetConfig.Registered() {
				performLicenseUpdate(ctx, objAPI)
			}

			// Reset the timer for next cycle.
			licenseUpdateTimer.Reset(licUpdateCycle)
		}
	}
}

func performLicenseUpdate(ctx context.Context, objectAPI ObjectLayer) {
	// the subnet license renewal api renews the license only
	// if required e.g. when it is expiring soon
	url := globalSubnetConfig.BaseURL + licRenewPath

	resp, err := globalSubnetConfig.Post(url, nil)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("error from %s: %w", url, err))
		return
	}

	r := gjson.Parse(resp).Get("license_v2")
	if r.Index == 0 {
		logger.LogIf(ctx, fmt.Errorf("license not found in response from %s", url))
		return
	}

	lic := r.String()
	if lic == globalSubnetConfig.License {
		// license hasn't changed.
		return
	}

	kv := "subnet license=" + lic
	result, err := setConfigKV(ctx, objectAPI, []byte(kv))
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("error setting subnet license config: %w", err))
		return
	}

	if result.Dynamic {
		if err := applyDynamicConfigForSubSys(GlobalContext, objectAPI, result.Cfg, result.SubSys); err != nil {
			logger.LogIf(ctx, fmt.Errorf("error applying subnet dynamic config: %w", err))
			return
		}
		globalNotificationSys.SignalConfigReload(result.SubSys)
	}
}
