/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

// GatewaySSE captures gateway configuration for SSE types
type GatewaySSE []string

const (
	// GatewaySSES3 is set when SSE-S3 encryption needed on both gateway and backend
	GatewaySSES3 = "S3"
	// GatewaySSEC is set when SSE-C encryption needed on both gateway and backend
	GatewaySSEC = "C"
)

func (sse GatewaySSE) SSES3() bool {
	for _, v := range sse {
		if v == GatewaySSES3 {
			return true
		}
	}
	return false
}

func (sse GatewaySSE) SSEC() bool {
	for _, v := range sse {
		if v == GatewaySSEC {
			return true
		}
	}
	return false
}

func (sse GatewaySSE) IsSet() bool {
	return sse.SSES3() || sse.SSEC()
}
