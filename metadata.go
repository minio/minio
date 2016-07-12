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
	"net/http"
	"strings"
)

var (
	metadataBucket = ".metadata"
)

// PutObjectMetadata saved object's metadata
func PutObjectMetadata(storage StorageAPI, bucket, object string, metadata map[string]string) error {
	name := metadataFilename(bucket, object)
	bytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	storage.DeleteFile(metadataBucket, name)
	err = storage.AppendFile(metadataBucket, name, bytes)
	if err != nil {
		return err
	}
	return nil
}

// GetObjectMetadata returns object's metadata
func GetObjectMetadata(storage StorageAPI, bucket, object string) (map[string]string, error) {
	name := metadataFilename(bucket, object)
	bytes, err := storage.ReadAll(metadataBucket, name)
	if err != nil {
		return nil, err
	}
	metadata := make(map[string]string)
	json.Unmarshal(bytes, &metadata)
	return metadata, nil
}

// ExtractMetadata extracts metadata from an HTTP request
func ExtractMetadata(r *http.Request) map[string]string {
	metadata := make(map[string]string)
	// Save other metadata if available.
	metadata["Content-Type"] = r.Header.Get("Content-Type")
	metadata["Content-Encoding"] = r.Header.Get("Content-Encoding")
	for key, value := range r.Header {
		for _, val := range value {
			k, v := handleMetadataHeader(metadata, key, val)
			if v != "" {
				metadata[k] = v
			}
		}
	}
	return metadata
}

func handleMetadataHeader(metadata map[string]string, key, value string) (k, v string) {
	k = http.CanonicalHeaderKey(key)
	if strings.HasPrefix(k, "X-Amz-Meta-") {
		v = metadataKeyMerge(metadata, k, value)
	} else if strings.HasPrefix(k, "X-Minio-Meta-") {
		v = metadataKeyMerge(metadata, k, value)
	} else {
		v = ""
	}
	return
}

func metadataKeyMerge(metadata map[string]string, key, value string) string {
	if val, ok := metadata[key]; ok {
		return val + "," + value
	}
	return value
}

func metadataFilename(bucket, object string) string {
	return pathJoin(bucket, object)
}
