/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"net/http"
	"strings"
)

// guessIsLoginSTSReq - returns true if incoming request is Login STS user
func guessIsLoginSTSReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return strings.HasPrefix(req.URL.Path, loginPathPrefix) ||
		(req.Method == http.MethodPost && req.URL.Path == SlashSeparator &&
			getRequestAuthType(req) == authTypeSTS)
}
