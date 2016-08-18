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

// GenericReply represents any generic RPC reply.
type GenericReply struct{}

// GenericArgs represents any generic RPC arguments.
type GenericArgs struct{}

// ListVolsReply represents list of vols RPC reply.
type ListVolsReply struct {
	// List of volumes stat information.
	Vols []VolInfo
}

// ReadAllArgs represents read all RPC arguments.
type ReadAllArgs struct {
	// Name of the volume.
	Vol string

	// Name of the path.
	Path string
}

// ReadFileArgs represents read file RPC arguments.
type ReadFileArgs struct {
	// Name of the volume.
	Vol string

	// Name of the path.
	Path string

	// Starting offset to start reading into Buffer.
	Offset int64

	// Data buffer read from the path at offset.
	Buffer []byte
}

// AppendFileArgs represents append file RPC arguments.
type AppendFileArgs struct {
	// Name of the volume.
	Vol string

	// Name of the path.
	Path string

	// Data buffer to be saved at path.
	Buffer []byte
}

// StatFileArgs represents stat file RPC arguments.
type StatFileArgs struct {
	// Name of the volume.
	Vol string

	// Name of the path.
	Path string
}

// DeleteFileArgs represents delete file RPC arguments.
type DeleteFileArgs struct {
	// Name of the volume.
	Vol string

	// Name of the path.
	Path string
}

// ListDirArgs represents list contents RPC arguments.
type ListDirArgs struct {
	// Name of the volume.
	Vol string

	// Name of the path.
	Path string
}

// RenameFileArgs represents rename file RPC arguments.
type RenameFileArgs struct {
	// Name of source volume.
	SrcVol string

	// Source path to be renamed.
	SrcPath string

	// Name of destination volume.
	DstVol string

	// Destination path of renamed file.
	DstPath string
}
