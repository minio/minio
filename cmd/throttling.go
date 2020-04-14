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

package cmd

import (
	"net/http"
	"sync"
	"time"
)

type apiThrottling struct {
	mu      sync.RWMutex
	enabled bool

	requestsDeadline time.Duration
	requestsPool     chan struct{}
}

func (t *apiThrottling) init(max int, deadline time.Duration) {
	if max <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.requestsPool = make(chan struct{}, max)
	t.requestsDeadline = deadline
	t.enabled = true
}

func (t *apiThrottling) get() (chan struct{}, <-chan time.Time) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.enabled {
		return nil, nil
	}

	return t.requestsPool, time.NewTimer(t.requestsDeadline).C
}

// maxClients throttles the S3 API calls
func maxClients(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pool, deadlineTimer := globalAPIThrottling.get()
		if pool == nil {
			f.ServeHTTP(w, r)
			return
		}

		select {
		case pool <- struct{}{}:
			defer func() { <-pool }()
			f.ServeHTTP(w, r)
		case <-deadlineTimer:
			// Send a http timeout message
			writeErrorResponse(r.Context(), w,
				errorCodes.ToAPIErr(ErrOperationMaxedOut),
				r.URL, guessIsBrowserReq(r))
			return
		case <-r.Context().Done():
			return
		}
	}
}
