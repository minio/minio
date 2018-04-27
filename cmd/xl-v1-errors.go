/*
 * Minio Cloud Storage, (C) 2016, 2018 Minio, Inc.
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

import "errors"

// errXLReadQuorum - did not meet read quorum.
var errXLReadQuorum = errors.New("Read failed. Insufficient number of disks online")

// errXLWriteQuorum - did not meet write quorum.
var errXLWriteQuorum = errors.New("Write failed. Insufficient number of disks online")

// errNoHealRequired - returned when healing is attempted on a previously healed disks.
var errNoHealRequired = errors.New("No healing is required")
