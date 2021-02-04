/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package target

import (
	"database/sql"
	"testing"
)

// TestPostgreSQLRegistration checks if sql driver
// is registered and fails otherwise.
func TestMySQLRegistration(t *testing.T) {
	var found bool
	for _, drv := range sql.Drivers() {
		if drv == "mysql" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("mysql driver not registered")
	}
}
