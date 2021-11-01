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
	"context"
	"errors"
	"net/http"
	"strconv"

	xhttp "github.com/minio/minio/internal/http"
)

const unavailable = "offline"

func shouldProxy() bool {
	return newObjectLayerFn() == nil
}

// ClusterCheckHandler returns if the server is ready for requests.
func ClusterCheckHandler(w http.ResponseWriter, r *http.Request) {
	if globalIsGateway {
		writeResponse(w, http.StatusOK, nil, mimeNone)
		return
	}

	ctx := newContext(r, w, "ClusterCheckHandler")

	if shouldProxy() {
		w.Header().Set(xhttp.MinIOServerStatus, unavailable)
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	objLayer := newObjectLayerFn()

	ctx, cancel := context.WithTimeout(ctx, globalAPIConfig.getClusterDeadline())
	defer cancel()

	opts := HealthOptions{Maintenance: r.Form.Get("maintenance") == "true"}
	result := objLayer.Health(ctx, opts)
	if result.WriteQuorum > 0 {
		w.Header().Set(xhttp.MinIOWriteQuorum, strconv.Itoa(result.WriteQuorum))
	}
	if !result.Healthy {
		// return how many drives are being healed if any
		if result.HealingDrives > 0 {
			w.Header().Set(xhttp.MinIOHealingDrives, strconv.Itoa(result.HealingDrives))
		}
		// As a maintenance call we are purposefully asked to be taken
		// down, this is for orchestrators to know if we can safely
		// take this server down, return appropriate error.
		if opts.Maintenance {
			writeResponse(w, http.StatusPreconditionFailed, nil, mimeNone)
		} else {
			writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		}
		return
	}
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// ClusterReadCheckHandler returns if the server is ready for requests.
func ClusterReadCheckHandler(w http.ResponseWriter, r *http.Request) {
	if globalIsGateway {
		writeResponse(w, http.StatusOK, nil, mimeNone)
		return
	}

	ctx := newContext(r, w, "ClusterReadCheckHandler")

	if shouldProxy() {
		w.Header().Set(xhttp.MinIOServerStatus, unavailable)
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	objLayer := newObjectLayerFn()

	ctx, cancel := context.WithTimeout(ctx, globalAPIConfig.getClusterDeadline())
	defer cancel()

	result := objLayer.ReadHealth(ctx)
	if !result {
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return
	}

	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// ReadinessCheckHandler Checks if the process is up. Always returns success.
func ReadinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	LivenessCheckHandler(w, r)
}

// LivenessCheckHandler - Checks if the process is up. Always returns success.
func LivenessCheckHandler(w http.ResponseWriter, r *http.Request) {
	if shouldProxy() {
		// Service not initialized yet
		w.Header().Set(xhttp.MinIOServerStatus, unavailable)
	}

	if globalIsGateway {
		objLayer := newObjectLayerFn()
		if objLayer == nil {
			apiErr := toAPIError(r.Context(), errServerNotInitialized)
			switch r.Method {
			case http.MethodHead:
				writeResponse(w, apiErr.HTTPStatusCode, nil, mimeNone)
			case http.MethodGet:
				writeErrorResponse(r.Context(), w, apiErr, r.URL)
			}
			return
		}

		storageInfo, _ := objLayer.StorageInfo(r.Context())
		if !storageInfo.Backend.GatewayOnline {
			err := errors.New("gateway backend is not reachable")
			apiErr := toAPIError(r.Context(), err)
			switch r.Method {
			case http.MethodHead:
				writeResponse(w, apiErr.HTTPStatusCode, nil, mimeNone)
			case http.MethodGet:
				writeErrorResponse(r.Context(), w, apiErr, r.URL)
			}
			return
		}

		if globalEtcdClient != nil {
			// Borrowed from
			// https://github.com/etcd-io/etcd/blob/main/etcdctl/ctlv3/command/ep_command.go#L118
			ctx, cancel := context.WithTimeout(r.Context(), defaultContextTimeout)
			defer cancel()
			if _, err := globalEtcdClient.Get(ctx, "health"); err != nil {
				// etcd unreachable throw an error..
				switch r.Method {
				case http.MethodHead:
					apiErr := toAPIError(r.Context(), err)
					writeResponse(w, apiErr.HTTPStatusCode, nil, mimeNone)
				case http.MethodGet:
					writeErrorResponse(r.Context(), w, toAPIError(r.Context(), err), r.URL)
				}
				return
			}
		}
	}

	writeResponse(w, http.StatusOK, nil, mimeNone)
}
