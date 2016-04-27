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

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// error type when key is not found.
var errMetadataKeyNotExist = errors.New("Key not found in xlMetadata.")

// error type when key is malformed, or doesn't contain the required
// data type.
var errMetadataFormatErr = errors.New("Key cannot be decoded to xlMetadata.")

// A xlMetadata represents a metadata header mapping
// keys to sets of values.
type xlMetadata map[string]interface{}

// Write writes a metadata in wire format.
func (f xlMetadata) Write(writer io.Writer) error {
	metadataBytes, err := json.Marshal(f)
	if err != nil {
		return err
	}
	_, err = writer.Write(metadataBytes)
	return err
}

type erasureInfo struct {
	dataBlocks   int
	parityBlocks int
	blockSize    int64
}

func (f xlMetadata) SetXLErasureInfo(eInfo erasureInfo) {
	f["xl.erasure"] = map[string]interface{}{
		"data":      eInfo.dataBlocks,
		"parity":    eInfo.parityBlocks,
		"blockSize": eInfo.blockSize,
	}
}

func (f xlMetadata) GetXLErasureInfo() (eInfo erasureInfo, err error) {
	xlErasureParams, ok := f["xl.erasure"]
	if !ok {
		return erasureInfo{}, errMetadataKeyNotExist
	}
	erasureParams, ok := xlErasureParams.(map[string]interface{})
	if !ok {
		return erasureInfo{}, errMetadataFormatErr
	}
	eInfo.dataBlocks, ok = erasureParams["data"].(int)
	if !ok {
		return erasureInfo{}, errMetadataFormatErr
	}
	eInfo.parityBlocks, ok = erasureParams["parity"].(int)
	if !ok {
		return erasureInfo{}, errMetadataFormatErr
	}
	eInfo.blockSize, ok = erasureParams["blockSize"].(int64)
	if !ok {
		return erasureInfo{}, errMetadataFormatErr
	}
	return eInfo, nil
}

func (f xlMetadata) GetXLVersion() (string, error) {
	xlVersion, ok := f["xl.version"]
	if !ok {
		return "", errMetadataKeyNotExist
	}
	xlVersionStr, ok := xlVersion.(string)
	if !ok {
		return "", errMetadataFormatErr
	}
	return xlVersionStr, nil
}

func (f xlMetadata) SetXLVersion(version string) {
	f["xl.version"] = version
}

// Get file info.
func (f xlMetadata) GetFileInfo() (FileInfo, error) {
	xlFile, ok := f["xl.file"]
	if !ok {
		return FileInfo{}, errMetadataKeyNotExist
	}
	fileInfo, ok := xlFile.(map[string]interface{})
	if !ok {
		return FileInfo{}, errMetadataFormatErr
	}
	fi := FileInfo{}
	fi.Size, ok = fileInfo["size"].(int64)
	if !ok {
		return FileInfo{}, errMetadataFormatErr
	}
	modTimeStr, ok := fileInfo["modTime"].(string)
	if !ok {
		return FileInfo{}, errMetadataFormatErr
	}
	var err error
	fi.ModTime, err = time.Parse(timeFormatAMZ, modTimeStr)
	if err != nil {
		return FileInfo{}, err
	}
	return fi, nil
}

// Set file size.
func (f xlMetadata) SetFileInfo(fileInfo FileInfo) {
	f["xl.file"] = map[string]interface{}{
		"size":    fileInfo.Size,
		"modTime": fileInfo.ModTime.Format(timeFormatAMZ),
	}
	fmt.Println(f)
}

// Get file version.
func (f xlMetadata) GetFileVersion() (int64, error) {
	xlFileVersion, ok := f["xl.file.Version"]
	if !ok {
		return 0, errMetadataKeyNotExist
	}
	version, ok := xlFileVersion.(int64)
	if !ok {
		return 0, errMetadataFormatErr
	}
	return version, nil
}

// Set file version.
func (f xlMetadata) SetFileVersion(fileVersion int64) {
	f["xl.file.version"] = fileVersion
}

// xlMetadataDecode - xl metadata decode.
func xlMetadataDecode(reader io.Reader) (xlMetadata, error) {
	metadata := make(xlMetadata)
	decoder := json.NewDecoder(reader)
	// Unmarshalling failed, file possibly corrupted.
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}
