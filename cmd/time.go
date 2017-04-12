/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

import "time"

var minioTZ = map[string]int{

	"PST": -8 * 3600,
	"PDT": -7 * 3600,

	"EST": -5 * 3600,
	"EDT": -4 * 3600,

	"GMT": 0,
	"UTC": 0,

	"CET":  1 * 3600,
	"CEST": 2 * 3600,
}

// parseTime returns the correct time when a timezone is provided
// with the time string, if the specified timezone is not found in
// minioTZ, call only the standard golang time parsing.
func parseTime(layout, value string) (time.Time, error) {
	// Standard golang time parsing
	t, err := time.Parse(layout, value)
	if err != nil {
		return time.Time{}, err
	}

	// Fetch the location of the passed time
	loc := t.Location()

	if loc == nil || loc == time.UTC || loc == time.Local {
		// Nothing to do, even when location is set to time.Local
		// since time will be obviously correct with the local
		// machine's timezone
		return t, nil
	}

	// Fetch the time offset associated to the passed timezone.
	// If not found, return golang std time parser result.
	offset, ok := minioTZ[loc.String()]
	if !ok {
		return t, nil
	}

	// Calculate the new time
	newTime := t.Add(-time.Duration(offset) * time.Second)

	return newTime, nil
}
