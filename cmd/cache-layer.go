/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

import (
	"io"
)

type cacheLayer struct {
	GatewayLayer
	cachePath string
}

func newCacheLayer(gl GatewayLayer, cachePath string) GatewayLayer {
	return &cacheLayer{
		GatewayLayer: gl,
		cachePath:    cachePath,
	}
}

func (cl *cacheLayer) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	// TODO(nl5887): add pipewriter hereo, make sure it is being written completely
	// use lock files, and meta for cache duration
	return cl.GatewayLayer.GetObject(bucket, object, startOffset, length, writer)
}

func (cl *cacheLayer) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return cl.GatewayLayer.GetObjectInfo(bucket, object)
}
