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

import "testing"

func TestGetInterfaceIPv4s(t *testing.T) {
	ipv4s, err := getInterfaceIPv4s()
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}
	for _, ip := range ipv4s {
		if ip.To4() == nil {
			t.Fatalf("Unexpected expecting only IPv4 addresses only %s", ip)
		}
	}
}
