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

	"github.com/minio/minio/cmd/config/api"
)

type apiConfig struct {
	mu sync.RWMutex

	requestsDeadline time.Duration
	requestsPool     chan struct{}
	readyDeadline    time.Duration
	corsAllowOrigins []string
}

func (t *apiConfig) init(cfg api.Config) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.readyDeadline = cfg.APIReadyDeadline
	t.corsAllowOrigins = cfg.APICorsAllowOrigin
	if cfg.APIRequestsMax <= 0 {
		return
	}

	apiRequestsMax := cfg.APIRequestsMax
	if len(globalEndpoints.Hosts()) > 0 {
		apiRequestsMax /= len(globalEndpoints.Hosts())
	}

	t.requestsPool = make(chan struct{}, apiRequestsMax)
	t.requestsDeadline = cfg.APIRequestsDeadline
}

func (t *apiConfig) getCorsAllowOrigins() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.corsAllowOrigins
}

func (t *apiConfig) getReadyDeadline() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.readyDeadline == 0 {
		return 10 * time.Second
	}

	return t.readyDeadline
}

func (t *apiConfig) getRequestsPool() (chan struct{}, <-chan time.Time) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.requestsPool == nil {
		return nil, nil
	}

	return t.requestsPool, time.NewTimer(t.requestsDeadline).C
}

// maxClients throttles the S3 API calls
func maxClients(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pool, deadlineTimer := globalAPIConfig.getRequestsPool()
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
