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

package main

import "errors"

// errMaxDisks - returned for reached maximum of disks.
var errMaxDisks = errors.New("Total number of disks specified is higher than supported maximum of '16'")

// errNumDisks - returned for odd numebr of disks.
var errNumDisks = errors.New("Invalid number of disks provided, should be always multiples of '2'")

// errUnexpected - returned for any unexpected error.
var errUnexpected = errors.New("Unexpected error - please report at https://github.com/minio/minio/issues")
