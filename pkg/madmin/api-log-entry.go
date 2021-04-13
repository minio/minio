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
	Metadata map[string]string `json:"metadata,omitempty"`
	Bucket   string            `json:"bucket,omitempty"`
	Object   string            `json:"object,omitempty"`
}

// Trace - defines the trace.
type logTrace struct {
	Variables map[string]string `json:"variables,omitempty"`
	Message   string            `json:"message,omitempty"`
	Source    []string          `json:"source,omitempty"`
}

// API - defines the api type and its args.
type logAPI struct {
	Args *logArgs `json:"args,omitempty"`
	Name string   `json:"name,omitempty"`
}

// Entry - defines fields and values of each log entry.
type logEntry struct {
	API          *logAPI   `json:"api,omitempty"`
	Trace        *logTrace `json:"error,omitempty"`
	DeploymentID string    `json:"deploymentid,omitempty"`
	Level        string    `json:"level"`
	LogKind      string    `json:"errKind"`
	Time         string    `json:"time"`
	RemoteHost   string    `json:"remotehost,omitempty"`
	Host         string    `json:"host,omitempty"`
	RequestID    string    `json:"requestID,omitempty"`
	UserAgent    string    `json:"userAgent,omitempty"`
	Message      string    `json:"message,omitempty"`
}
