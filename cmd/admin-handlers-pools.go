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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/internal/logger"
	iampolicy "github.com/minio/pkg/iam/policy"
)

var (
	errRebalanceDecommissionAlreadyRunning = errors.New("Rebalance cannot be started, decommission is aleady in progress")
	errDecommissionRebalanceAlreadyRunning = errors.New("Decommission cannot be started, rebalance is already in progress")
)

func (a adminAPIHandlers) StartDecommission(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "StartDecommission")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.DecommissionAdminAction)
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

	if pools.IsRebalanceStarted() {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errDecommissionRebalanceAlreadyRunning), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]

	idx := globalEndpoints.GetPoolIdx(v)
	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	if ep := globalEndpoints[idx].Endpoints[0]; !ep.IsLocal {
		for nodeIdx, proxyEp := range globalProxyEndpoints {
			if proxyEp.Endpoint.Host == ep.Host {
				if proxyRequestByNodeIndex(ctx, w, r, nodeIdx) {
					return
				}
			}
		}
	}

	if err := pools.Decommission(r.Context(), idx); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func (a adminAPIHandlers) CancelDecommission(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CancelDecommission")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.DecommissionAdminAction)
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

	idx := globalEndpoints.GetPoolIdx(v)
	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	if ep := globalEndpoints[idx].Endpoints[0]; !ep.IsLocal {
		for nodeIdx, proxyEp := range globalProxyEndpoints {
			if proxyEp.Endpoint.Host == ep.Host {
				if proxyRequestByNodeIndex(ctx, w, r, nodeIdx) {
					return
				}
			}
		}
	}

	if err := pools.DecommissionCancel(ctx, idx); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func (a adminAPIHandlers) StatusPool(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "StatusPool")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ServerInfoAdminAction, iampolicy.DecommissionAdminAction)
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

	idx := globalEndpoints.GetPoolIdx(v)
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

	logger.LogIf(r.Context(), json.NewEncoder(w).Encode(&status))
}

func (a adminAPIHandlers) ListPools(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListPools")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.ServerInfoAdminAction, iampolicy.DecommissionAdminAction)
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

	logger.LogIf(r.Context(), json.NewEncoder(w).Encode(poolsStatus))
}

func (a adminAPIHandlers) RebalanceStart(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RebalanceStart")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.RebalanceAdminAction)
	if objectAPI == nil {
		return
	}

	// NB rebalance-start admin API is always coordinated from first pool's
	// first node. The following is required to serialize (the effects of)
	// concurrent rebalance-start commands.
	if ep := globalEndpoints[0].Endpoints[0]; !ep.IsLocal {
		for nodeIdx, proxyEp := range globalProxyEndpoints {
			if proxyEp.Endpoint.Host == ep.Host {
				if proxyRequestByNodeIndex(ctx, w, r, nodeIdx) {
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

	if pools.IsRebalanceStarted() {
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
	ctx := newContext(r, w, "RebalanceStatus")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.RebalanceAdminAction)
	if objectAPI == nil {
		return
	}

	// Proxy rebalance-status to first pool first node, so that users see a
	// consistent view of rebalance progress even though different rebalancing
	// pools may temporarily have out of date info on the others.
	if ep := globalEndpoints[0].Endpoints[0]; !ep.IsLocal {
		for nodeIdx, proxyEp := range globalProxyEndpoints {
			if proxyEp.Endpoint.Host == ep.Host {
				if proxyRequestByNodeIndex(ctx, w, r, nodeIdx) {
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
		if errors.Is(err, errRebalanceNotStarted) {
			writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrAdminRebalanceNotStarted), r.URL)
			return
		}
		logger.LogIf(ctx, fmt.Errorf("failed to fetch rebalance status: %w", err))
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	logger.LogIf(r.Context(), json.NewEncoder(w).Encode(rs))
}

func (a adminAPIHandlers) RebalanceStop(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "RebalanceStop")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI, _ := validateAdminReq(ctx, w, r, iampolicy.RebalanceAdminAction)
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
	logger.LogIf(ctx, pools.saveRebalanceStats(GlobalContext, 0, rebalSaveStoppedAt))
}
