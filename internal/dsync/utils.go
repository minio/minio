// Copyright (c) 2015-2021 MinIO, Inc.
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

package dsync

import (
	"math/rand"
	"time"
)

func backoffWait(minSleep, unit, maxSleep time.Duration) func(*rand.Rand, uint) time.Duration {
	if unit > time.Hour {
		// Protect against integer overflow
		panic("unit cannot exceed one hour")
	}
	return func(r *rand.Rand, attempt uint) time.Duration {
		sleep := minSleep
		sleep += unit * time.Duration(attempt)
		if sleep > maxSleep {
			sleep = maxSleep
		}
		sleep -= time.Duration(r.Float64() * float64(sleep))
		return sleep
	}
}
