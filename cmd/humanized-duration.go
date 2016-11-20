/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"
	"math"
	"time"
)

// humanizedDuration container to capture humanized time.
type humanizedDuration struct {
	Days    int64 `json:"days,omitempty"`
	Hours   int64 `json:"hours,omitempty"`
	Minutes int64 `json:"minutes,omitempty"`
	Seconds int64 `json:"seconds,omitempty"`
}

// StringShort() humanizes humanizedDuration to human readable short format.
// This does not print at seconds.
func (r humanizedDuration) StringShort() string {
	if r.Days == 0 && r.Hours == 0 {
		return fmt.Sprintf("%d minutes", r.Minutes)
	}
	if r.Days == 0 {
		return fmt.Sprintf("%d hours %d minutes", r.Hours, r.Minutes)
	}
	return fmt.Sprintf("%d days %d hours %d minutes", r.Days, r.Hours, r.Minutes)
}

// String() humanizes humanizedDuration to human readable,
func (r humanizedDuration) String() string {
	if r.Days == 0 && r.Hours == 0 && r.Minutes == 0 {
		return fmt.Sprintf("%d seconds", r.Seconds)
	}
	if r.Days == 0 && r.Hours == 0 {
		return fmt.Sprintf("%d minutes %d seconds", r.Minutes, r.Seconds)
	}
	if r.Days == 0 {
		return fmt.Sprintf("%d hours %d minutes %d seconds", r.Hours, r.Minutes, r.Seconds)
	}
	return fmt.Sprintf("%d days %d hours %d minutes %d seconds", r.Days, r.Hours, r.Minutes, r.Seconds)
}

// timeDurationToHumanizedDuration convert golang time.Duration to a custom more readable humanizedDuration.
func timeDurationToHumanizedDuration(duration time.Duration) humanizedDuration {
	r := humanizedDuration{}
	if duration.Seconds() < 60.0 {
		r.Seconds = int64(duration.Seconds())
		return r
	}
	if duration.Minutes() < 60.0 {
		remainingSeconds := math.Mod(duration.Seconds(), 60)
		r.Seconds = int64(remainingSeconds)
		r.Minutes = int64(duration.Minutes())
		return r
	}
	if duration.Hours() < 24.0 {
		remainingMinutes := math.Mod(duration.Minutes(), 60)
		remainingSeconds := math.Mod(duration.Seconds(), 60)
		r.Seconds = int64(remainingSeconds)
		r.Minutes = int64(remainingMinutes)
		r.Hours = int64(duration.Hours())
		return r
	}
	remainingHours := math.Mod(duration.Hours(), 24)
	remainingMinutes := math.Mod(duration.Minutes(), 60)
	remainingSeconds := math.Mod(duration.Seconds(), 60)
	r.Hours = int64(remainingHours)
	r.Minutes = int64(remainingMinutes)
	r.Seconds = int64(remainingSeconds)
	r.Days = int64(duration.Hours() / 24)
	return r
}
