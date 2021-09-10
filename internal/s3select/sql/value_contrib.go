/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import "time"

func timestampCompare(op string, left, right time.Time) bool {
	switch op {
	case opLt:
		return left.Before(right)
	case opLte:
		return left.Before(right) || left.Equal(right)
	case opGt:
		return left.After(right)
	case opGte:
		return left.After(right) || left.Equal(right)
	case opEq:
		return left.Equal(right)
	case opIneq:
		return !left.Equal(right)
	}
	// This case does not happen
	return false
}
