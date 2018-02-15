/*
 * Minio Cloud Storage (C) 2016 Minio, Inc.
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

// File for all the browser constants.

// minioBrowserPrefix absolute path.
var p = window.location.pathname
export const minioBrowserPrefix = p.slice(0, p.indexOf("/", 1))

export const READ_ONLY = "readonly"
export const WRITE_ONLY = "writeonly"
export const READ_WRITE = "readwrite"

export const SHARE_OBJECT_EXPIRY_DAYS = 5
export const SHARE_OBJECT_EXPIRY_HOURS = 0
export const SHARE_OBJECT_EXPIRY_MINUTES = 0
