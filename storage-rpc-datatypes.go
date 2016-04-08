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

// GenericReply generic rpc reply.
type GenericReply struct{}

// GenericArgs generic rpc args.
type GenericArgs struct{}

// ListVolsReply list vols rpc reply.
type ListVolsReply struct {
	Vols []VolInfo
}

// ListFilesArgs list file args.
type ListFilesArgs struct {
	Vol       string
	Prefix    string
	Marker    string
	Recursive bool
	Count     int
}

// ListFilesReply list file reply.
type ListFilesReply struct {
	Files []FileInfo
	EOF   bool
}

// ReadFileArgs read file args.
type ReadFileArgs struct {
	Vol    string
	Path   string
	Offset int64
}

// ReadFileReply read file reply.
type ReadFileReply struct {
	URL string
}

// CreateFileArgs create file args.
type CreateFileArgs struct {
	Vol  string
	Path string
}

// CreateFileReply create file reply.
type CreateFileReply struct {
	URL string
}

// StatFileArgs stat file args.
type StatFileArgs struct {
	Vol  string
	Path string
}

// DeleteFileArgs delete file args.
type DeleteFileArgs struct {
	Vol  string
	Path string
}
