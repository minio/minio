// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"math"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/mem"

	"github.com/minio/minio/internal/config/api"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mcontext"
)

type apiConfig struct {
	mu sync.RWMutex

	requestsDeadline      time.Duration
	requestsPool          chan struct{}
	clusterDeadline       time.Duration
	listQuorum            string
	corsAllowOrigins      []string
	replicationPriority   string
	replicationMaxWorkers int
	transitionWorkers     int

	staleUploadsExpiry          time.Duration
	staleUploadsCleanupInterval time.Duration
	deleteCleanupInterval       time.Duration
	enableODirect               bool
	gzipObjects                 bool
	rootAccess                  bool
	syncEvents                  bool
	objectMaxVersions           int64
}

const (
	cgroupV1MemLimitFile = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cgroupV2MemLimitFile = "/sys/fs/cgroup/memory.max"
	cgroupMemNoLimit     = 9223372036854771712
)

func cgroupMemLimit() (limit uint64) {
	buf, err := os.ReadFile(cgroupV2MemLimitFile)
	if err != nil {
		buf, err = os.ReadFile(cgroupV1MemLimitFile)
	}
	if err != nil {
		return 0
	}
	limit, err = strconv.ParseUint(strings.TrimSpace(string(buf)), 10, 64)
	if err != nil {
		// The kernel can return valid but non integer values
		// but still, no need to interpret more
		return 0
	}
	if limit == cgroupMemNoLimit {
		// No limit set, It's the highest positive signed 64-bit
		// integer (2^63-1), rounded down to multiples of 4096 (2^12),
		// the most common page size on x86 systems - for cgroup_limits.
		return 0
	}
	return limit
}

func availableMemory() (available uint64) {
	available = 2048 * blockSizeV2 * 2 // Default to 4 GiB when we can't find the limits.

	if runtime.GOOS == "linux" {
		// Useful in container mode
		limit := cgroupMemLimit()
		if limit > 0 {
			// A valid value is found, return its 75%
			available = (limit * 3) / 4
			return
		}
	} // for all other platforms limits are based on virtual memory.

	memStats, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	// A valid value is available return its 75%
	available = (memStats.Available * 3) / 4
	return
}

func (t *apiConfig) init(cfg api.Config, setDriveCounts []int, legacy bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	clusterDeadline := cfg.ClusterDeadline
	if clusterDeadline == 0 {
		clusterDeadline = 10 * time.Second
	}
	t.clusterDeadline = clusterDeadline
	corsAllowOrigin := cfg.CorsAllowOrigin
	if len(corsAllowOrigin) == 0 {
		corsAllowOrigin = []string{"*"}
	}
	t.corsAllowOrigins = corsAllowOrigin

	var apiRequestsMaxPerNode int
	if cfg.RequestsMax <= 0 {
		maxSetDrives := slices.Max(setDriveCounts)

		// Returns 75% of max memory allowed
		maxMem := availableMemory()

		// max requests per node is calculated as
		// total_ram / ram_per_request
		blockSize := xioutil.LargeBlock + xioutil.SmallBlock
		if legacy {
			// ram_per_request is (1MiB+32KiB) * driveCount \
			//    + 2 * 10MiB (default erasure block size v1) + 2 * 1MiB (default erasure block size v2)
			apiRequestsMaxPerNode = int(maxMem / uint64(maxSetDrives*blockSize+int(blockSizeV1*2+blockSizeV2*2)))
		} else {
			// ram_per_request is (1MiB+32KiB) * driveCount \
			//    + 2 * 1MiB (default erasure block size v2)
			apiRequestsMaxPerNode = int(maxMem / uint64(maxSetDrives*blockSize+int(blockSizeV2*2)))
		}
	} else {
		apiRequestsMaxPerNode = cfg.RequestsMax
		if n := totalNodeCount(); n > 0 {
			apiRequestsMaxPerNode /= n
		}
	}

	if globalIsDistErasure {
		logger.Info("Configured max API requests per node based on available memory: %d", apiRequestsMaxPerNode)
	}

	if cap(t.requestsPool) != apiRequestsMaxPerNode {
		// Only replace if needed.
		// Existing requests will use the previous limit,
		// but new requests will use the new limit.
		// There will be a short overlap window,
		// but this shouldn't last long.
		t.requestsPool = make(chan struct{}, apiRequestsMaxPerNode)
	}
	t.requestsDeadline = cfg.RequestsDeadline
	listQuorum := cfg.ListQuorum
	if listQuorum == "" {
		listQuorum = "strict"
	}
	t.listQuorum = listQuorum
	if globalReplicationPool != nil &&
		(cfg.ReplicationPriority != t.replicationPriority || cfg.ReplicationMaxWorkers != t.replicationMaxWorkers) {
		globalReplicationPool.ResizeWorkerPriority(cfg.ReplicationPriority, cfg.ReplicationMaxWorkers)
	}
	t.replicationPriority = cfg.ReplicationPriority
	t.replicationMaxWorkers = cfg.ReplicationMaxWorkers

	// N B api.transition_workers will be deprecated
	if globalTransitionState != nil {
		globalTransitionState.UpdateWorkers(cfg.TransitionWorkers)
	}
	t.transitionWorkers = cfg.TransitionWorkers

	t.staleUploadsExpiry = cfg.StaleUploadsExpiry
	t.staleUploadsCleanupInterval = cfg.StaleUploadsCleanupInterval
	t.deleteCleanupInterval = cfg.DeleteCleanupInterval
	t.enableODirect = cfg.EnableODirect
	t.gzipObjects = cfg.GzipObjects
	t.rootAccess = cfg.RootAccess
	t.syncEvents = cfg.SyncEvents
	t.objectMaxVersions = cfg.ObjectMaxVersions
}

func (t *apiConfig) odirectEnabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.enableODirect
}

func (t *apiConfig) shouldGzipObjects() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.gzipObjects
}

func (t *apiConfig) permitRootAccess() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.rootAccess
}

func (t *apiConfig) getListQuorum() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.listQuorum == "" {
		return "strict"
	}

	return t.listQuorum
}

func (t *apiConfig) getCorsAllowOrigins() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.corsAllowOrigins) == 0 {
		return []string{"*"}
	}

	corsAllowOrigins := make([]string, len(t.corsAllowOrigins))
	copy(corsAllowOrigins, t.corsAllowOrigins)
	return corsAllowOrigins
}

func (t *apiConfig) getStaleUploadsCleanupInterval() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.staleUploadsCleanupInterval == 0 {
		return 6 * time.Hour // default 6 hours
	}

	return t.staleUploadsCleanupInterval
}

func (t *apiConfig) getStaleUploadsExpiry() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.staleUploadsExpiry == 0 {
		return 24 * time.Hour // default 24 hours
	}

	return t.staleUploadsExpiry
}

func (t *apiConfig) getDeleteCleanupInterval() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.deleteCleanupInterval == 0 {
		return 5 * time.Minute // every 5 minutes
	}

	return t.deleteCleanupInterval
}

func (t *apiConfig) getClusterDeadline() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.clusterDeadline == 0 {
		return 10 * time.Second
	}

	return t.clusterDeadline
}

func (t *apiConfig) getRequestsPoolCapacity() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return cap(t.requestsPool)
}

func (t *apiConfig) getRequestsPool() (chan struct{}, time.Duration) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.requestsPool == nil {
		return nil, 10 * time.Second
	}

	if t.requestsDeadline <= 0 {
		return t.requestsPool, 10 * time.Second
	}

	return t.requestsPool, t.requestsDeadline
}

// maxClients throttles the S3 API calls
func maxClients(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		globalHTTPStats.incS3RequestsIncoming()

		if r.Header.Get(globalObjectPerfUserMetadata) == "" {
			if val := globalServiceFreeze.Load(); val != nil {
				if unlock, ok := val.(chan struct{}); ok && unlock != nil {
					// Wait until unfrozen.
					select {
					case <-unlock:
					case <-r.Context().Done():
						// if client canceled we don't need to wait here forever.
						return
					}
				}
			}
		}

		pool, deadline := globalAPIConfig.getRequestsPool()
		if pool == nil {
			f.ServeHTTP(w, r)
			return
		}

		globalHTTPStats.addRequestsInQueue(1)

		if tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt); ok {
			tc.FuncName = "s3.MaxClients"
		}

		deadlineTimer := time.NewTimer(deadline)
		defer deadlineTimer.Stop()

		select {
		case pool <- struct{}{}:
			defer func() { <-pool }()
			globalHTTPStats.addRequestsInQueue(-1)
			f.ServeHTTP(w, r)
		case <-deadlineTimer.C:
			// Send a http timeout message
			writeErrorResponse(r.Context(), w,
				errorCodes.ToAPIErr(ErrTooManyRequests),
				r.URL)
			globalHTTPStats.addRequestsInQueue(-1)
			return
		case <-r.Context().Done():
			// When the client disconnects before getting the S3 handler
			// status code response, set the status code to 499 so this request
			// will be properly audited and traced.
			w.WriteHeader(499)
			globalHTTPStats.addRequestsInQueue(-1)
			return
		}
	}
}

func (t *apiConfig) getReplicationOpts() replicationPoolOpts {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.replicationPriority == "" {
		return replicationPoolOpts{
			Priority:   "auto",
			MaxWorkers: WorkerMaxLimit,
		}
	}

	return replicationPoolOpts{
		Priority:   t.replicationPriority,
		MaxWorkers: t.replicationMaxWorkers,
	}
}

func (t *apiConfig) getTransitionWorkers() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.transitionWorkers <= 0 {
		return runtime.GOMAXPROCS(0) / 2
	}

	return t.transitionWorkers
}

func (t *apiConfig) isSyncEventsEnabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.syncEvents
}

func (t *apiConfig) getObjectMaxVersions() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.objectMaxVersions <= 0 {
		// defaults to 'IntMax' when unset.
		return math.MaxInt64
	}

	return t.objectMaxVersions
}
