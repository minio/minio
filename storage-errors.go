/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

// errDiskPathFull - cannot create volume or files when disk is full.
var errDiskPathFull = errors.New("Disk path full.")

// errFileNotFound - cannot find the file.
var errFileNotFound = errors.New("File not found.")

// errVolumeExists - cannot create same volume again.
var errVolumeExists = errors.New("Volume already exists.")

// errIsNotRegular - not a regular file type.
var errIsNotRegular = errors.New("Not a regular file type.")

// errVolumeNotFound - cannot find the volume.
var errVolumeNotFound = errors.New("Volume not found.")
