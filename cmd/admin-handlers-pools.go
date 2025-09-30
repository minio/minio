// Copyright (c) 2015-2024 MinIO, Inc.
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/minio/mux"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/policy"
)

var (
	errRebalanceDecommissionAlreadyRunning = errors.New("Rebalance cannot be started, decommission is already in progress")
	errDecommissionRebalanceAlreadyRunning = errors.New("Decommission cannot be started, rebalance is already in progress")
)

func (a adminAPIHandlers) StartDecommission(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.DecommissionAdminAction)
	if objectAPI == nil {
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	z, ok := objectAPI.(*erasureServerPools)
	if !ok || len(z.serverPools) == 1 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if z.IsDecommissionRunning() {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errDecommissionAlreadyRunning), r.URL)
		return
	}

	if z.IsRebalanceStarted(ctx) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminRebalanceAlreadyStarted), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]
	byID := vars["by-id"] == "true"

	pools := strings.Split(v, ",")
	poolIndices := make([]int, 0, len(pools))

	for _, pool := range pools {
		var idx int
		if byID {
			var err error
			idx, err = strconv.Atoi(pool)
			if err != nil {
				// We didn't find any matching pools, invalid input
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
				return
			}
		} else {
			idx = globalEndpoints.GetPoolIdx(pool)
			if idx == -1 {
				// We didn't find any matching pools, invalid input
				writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
				return
			}
		}
		var pool *erasureSets
		for pidx := range z.serverPools {
			if pidx == idx {
				pool = z.serverPools[idx]
				break
			}
		}
		if pool == nil {
			// We didn't find any matching pools, invalid input
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
			return
		}

		poolIndices = append(poolIndices, idx)
	}

	if len(poolIndices) == 0 || !proxyDecommissionRequest(ctx, globalEndpoints[poolIndices[0]].Endpoints[0], w, r) {
		if err := z.Decommission(r.Context(), poolIndices...); err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
}

func (a adminAPIHandlers) CancelDecommission(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.DecommissionAdminAction)
	if objectAPI == nil {
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]
	byID := vars["by-id"] == "true"
	idx := -1

	if byID {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 && i < len(globalEndpoints) {
			idx = i
		}
	} else {
		idx = globalEndpoints.GetPoolIdx(v)
	}

	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	if !proxyDecommissionRequest(ctx, globalEndpoints[idx].Endpoints[0], w, r) {
		if err := pools.DecommissionCancel(ctx, idx); err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
	}
}

func (a adminAPIHandlers) StatusPool(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ServerInfoAdminAction, policy.DecommissionAdminAction)
	if objectAPI == nil {
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]
	byID := vars["by-id"] == "true"
	idx := -1

	if byID {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 && i < len(globalEndpoints) {
			idx = i
		}
	} else {
		idx = globalEndpoints.GetPoolIdx(v)
	}

	if idx == -1 {
		apiErr := toAdminAPIErr(ctx, errInvalidArgument)
		apiErr.Description = fmt.Sprintf("specified pool '%s' not found, please specify a valid pool", v)
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, apiErr, r.URL)
		return
	}

	status, err := pools.Status(r.Context(), idx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	adminLogIf(r.Context(), json.NewEncoder(w).Encode(&status))
}

func (a adminAPIHandlers) ListPools(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.ServerInfoAdminAction, policy.DecommissionAdminAction)
	if objectAPI == nil {
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	poolsStatus := make([]PoolStatus, len(globalEndpoints))
	for idx := range globalEndpoints {
		status, err := pools.Status(r.Context(), idx)
		if err != nil {
			writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
			return
		}
		poolsStatus[idx] = status
	}

	adminLogIf(r.Context(), json.NewEncoder(w).Encode(poolsStatus))
}

func (a adminAPIHandlers) RebalanceStart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.RebalanceAdminAction)
	if objectAPI == nil {
		return
	}

	// NB rebalance-start admin API is always coordinated from first pool's
	// first node. The following is required to serialize (the effects of)
	// concurrent rebalance-start commands.
	if ep := globalEndpoints[0].Endpoints[0]; !ep.IsLocal {
		for nodeIdx, proxyEp := range globalProxyEndpoints {
			if proxyEp.Host == ep.Host {
				if proxied, success := proxyRequestByNodeIndex(ctx, w, r, nodeIdx, false); proxied && success {
					return
				}
			}
		}
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok || len(pools.serverPools) == 1 {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if pools.IsDecommissionRunning() {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errRebalanceDecommissionAlreadyRunning), r.URL)
		return
	}

	if pools.IsRebalanceStarted(ctx) {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminRebalanceAlreadyStarted), r.URL)
		return
	}

	bucketInfos, err := objectAPI.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	buckets := make([]string, 0, len(bucketInfos))
	for _, bInfo := range bucketInfos {
		buckets = append(buckets, bInfo.Name)
	}

	var id string
	if id, err = pools.initRebalanceMeta(ctx, buckets); err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Rebalance routine is run on the first node of any pool participating in rebalance.
	pools.StartRebalance()

	b, err := json.Marshal(struct {
		ID string `json:"id"`
	}{ID: id})
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	writeSuccessResponseJSON(w, b)
	// Notify peers to load rebalance.bin and start rebalance routine if they happen to be
	// participating pool's leader node
	globalNotificationSys.LoadRebalanceMeta(ctx, true)
}

func (a adminAPIHandlers) RebalanceStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.RebalanceAdminAction)
	if objectAPI == nil {
		return
	}

	// Proxy rebalance-status to first pool first node, so that users see a
	// consistent view of rebalance progress even though different rebalancing
	// pools may temporarily have out of date info on the others.
	if ep := globalEndpoints[0].Endpoints[0]; !ep.IsLocal {
		for nodeIdx, proxyEp := range globalProxyEndpoints {
			if proxyEp.Host == ep.Host {
				if proxied, success := proxyRequestByNodeIndex(ctx, w, r, nodeIdx, false); proxied && success {
					return
				}
			}
		}
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	rs, err := rebalanceStatus(ctx, pools)
	if err != nil {
		if errors.Is(err, errRebalanceNotStarted) || errors.Is(err, errConfigNotFound) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminRebalanceNotStarted), r.URL)
			return
		}
		adminLogIf(ctx, fmt.Errorf("failed to fetch rebalance status: %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	adminLogIf(r.Context(), json.NewEncoder(w).Encode(rs))
}

func (a adminAPIHandlers) RebalanceStop(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	objectAPI, _ := validateAdminReq(ctx, w, r, policy.RebalanceAdminAction)
	if objectAPI == nil {
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	// Cancel any ongoing rebalance operation
	globalNotificationSys.StopRebalance(r.Context())
	writeSuccessResponseHeadersOnly(w)
	adminLogIf(ctx, pools.saveRebalanceStats(GlobalContext, 0, rebalSaveStoppedAt))
	globalNotificationSys.LoadRebalanceMeta(ctx, false)
}

func proxyDecommissionRequest(ctx context.Context, defaultEndPoint Endpoint, w http.ResponseWriter, r *http.Request) (proxy bool) {
	host := env.Get("_MINIO_DECOM_ENDPOINT_HOST", defaultEndPoint.Host)
	if host == "" {
		return proxy
	}
	for nodeIdx, proxyEp := range globalProxyEndpoints {
		if proxyEp.Host == host && !proxyEp.IsLocal {
			if proxied, success := proxyRequestByNodeIndex(ctx, w, r, nodeIdx, false); proxied && success {
				return true
			}
		}
	}
	return proxy
}
