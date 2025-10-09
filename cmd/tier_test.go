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
	"testing"
	"time"
)

func TestTierMetrics(t *testing.T) {
	tier := "WARM-1"
	globalTierMetrics.Observe(tier, 200*time.Millisecond)
	expSuccess := 10
	expFailure := 5
	for range expSuccess {
		globalTierMetrics.logSuccess(tier)
	}
	for range expFailure {
		globalTierMetrics.logFailure(tier)
	}
	metrics := globalTierMetrics.Report()
	var succ, fail float64
	for _, metric := range metrics {
		switch metric.Description.Name {
		case tierRequestsSuccess:
			succ += metric.Value
		case tierRequestsFailure:
			fail += metric.Value
		}
	}
	if int(succ) != expSuccess {
		t.Fatalf("Expected %d successes but got %f", expSuccess, succ)
	}
	if int(fail) != expFailure {
		t.Fatalf("Expected %d failures but got %f", expFailure, fail)
	}
}
