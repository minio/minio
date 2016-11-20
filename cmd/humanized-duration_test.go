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
	"strings"
	"testing"
	"time"
)

// Test humanized duration.
func TestHumanizedDuration(t *testing.T) {
	duration := time.Duration(90487000000000)
	humanDuration := timeDurationToHumanizedDuration(duration)
	if !strings.HasSuffix(humanDuration.String(), "seconds") {
		t.Fatal("Stringer method for humanized duration should have seconds.", humanDuration.String())
	}
	if strings.HasSuffix(humanDuration.StringShort(), "seconds") {
		t.Fatal("StringShorter method for humanized duration should not have seconds.", humanDuration.StringShort())
	}

	// Test humanized duration for seconds.
	humanSecDuration := timeDurationToHumanizedDuration(time.Duration(5 * time.Second))
	expectedHumanSecDuration := humanizedDuration{
		Seconds: 5,
	}
	if humanSecDuration != expectedHumanSecDuration {
		t.Fatalf("Expected %#v, got %#v incorrect conversion of duration to humanized form",
			expectedHumanSecDuration, humanSecDuration)
	}
	if strings.HasSuffix(humanSecDuration.String(), "days") ||
		strings.HasSuffix(humanSecDuration.String(), "hours") ||
		strings.HasSuffix(humanSecDuration.String(), "minutes") {
		t.Fatal("Stringer method for humanized duration should have only seconds.", humanSecDuration.String())
	}

	// Test humanized duration for minutes.
	humanMinDuration := timeDurationToHumanizedDuration(10 * time.Minute)
	expectedHumanMinDuration := humanizedDuration{
		Minutes: 10,
	}
	if humanMinDuration != expectedHumanMinDuration {
		t.Fatalf("Expected %#v, got %#v incorrect conversion of duration to humanized form",
			expectedHumanMinDuration, humanMinDuration)
	}
	if strings.HasSuffix(humanMinDuration.String(), "hours") {
		t.Fatal("Stringer method for humanized duration should have only minutes.", humanMinDuration.String())
	}

	// Test humanized duration for hours.
	humanHourDuration := timeDurationToHumanizedDuration(10 * time.Hour)
	expectedHumanHourDuration := humanizedDuration{
		Hours: 10,
	}
	if humanHourDuration != expectedHumanHourDuration {
		t.Fatalf("Expected %#v, got %#v incorrect conversion of duration to humanized form",
			expectedHumanHourDuration, humanHourDuration)
	}
	if strings.HasSuffix(humanHourDuration.String(), "days") {
		t.Fatal("Stringer method for humanized duration should have hours.", humanHourDuration.String())
	}
}
