/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package madmin

// Args - defines the arguments for the API.
type logArgs struct {
	Bucket   string            `json:"bucket,omitempty"`
	Object   string            `json:"object,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Trace - defines the trace.
type logTrace struct {
	Message   string            `json:"message,omitempty"`
	Source    []string          `json:"source,omitempty"`
	Variables map[string]string `json:"variables,omitempty"`
}

// API - defines the api type and its args.
type logAPI struct {
	Name string   `json:"name,omitempty"`
	Args *logArgs `json:"args,omitempty"`
}

// Entry - defines fields and values of each log entry.
type logEntry struct {
	DeploymentID string    `json:"deploymentid,omitempty"`
	Level        string    `json:"level"`
	LogKind      string    `json:"errKind"`
	Time         string    `json:"time"`
	API          *logAPI   `json:"api,omitempty"`
	RemoteHost   string    `json:"remotehost,omitempty"`
	Host         string    `json:"host,omitempty"`
	RequestID    string    `json:"requestID,omitempty"`
	UserAgent    string    `json:"userAgent,omitempty"`
	Message      string    `json:"message,omitempty"`
	Trace        *logTrace `json:"error,omitempty"`
}
