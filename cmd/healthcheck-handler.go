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
	"net/http"
	"strconv"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
)

const unavailable = "offline"

func checkHealth(w http.ResponseWriter) ObjectLayer {
	objLayer := newObjectLayerFn()
	if objLayer == nil {
		w.Header().Set(xhttp.MinIOServerStatus, unavailable)
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return nil
	}

	if !globalBucketMetadataSys.Initialized() {
		w.Header().Set(xhttp.MinIOServerStatus, "bucket-metadata-offline")
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return nil
	}

	if !globalIAMSys.Initialized() {
		w.Header().Set(xhttp.MinIOServerStatus, "iam-offline")
		writeResponse(w, http.StatusServiceUnavailable, nil, mimeNone)
		return nil
	}

	return objLayer
}

// ClusterCheckHandler returns if the server is ready for requests.
func ClusterCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ClusterCheckHandler")

	objLayer := checkHealth(w)
	if objLayer == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, globalAPIConfig.getClusterDeadline())
	defer cancel()

	opts := HealthOptions{
		Maintenance:    r.Form.Get("maintenance") == "true",
		DeploymentType: r.Form.Get("deployment-type"),
	}
	result := objLayer.Health(ctx, opts)
	w.Header().Set(xhttp.MinIOWriteQuorum, strconv.Itoa(result.WriteQuorum))
	w.Header().Set(xhttp.MinIOStorageClassDefaults, strconv.FormatBool(result.UsingDefaults))
	// return how many drives are being healed if any
	if result.HealingDrives > 0 {
		w.Header().Set(xhttp.MinIOHealingDrives, strconv.Itoa(result.HealingDrives))
	}
	if !result.Healthy {
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
	ctx := newContext(r, w, "ClusterReadCheckHandler")

	objLayer := checkHealth(w)
	if objLayer == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, globalAPIConfig.getClusterDeadline())
	defer cancel()

	opts := HealthOptions{
		Maintenance:    r.Form.Get("maintenance") == "true",
		DeploymentType: r.Form.Get("deployment-type"),
	}
	result := objLayer.Health(ctx, opts)
	w.Header().Set(xhttp.MinIOReadQuorum, strconv.Itoa(result.ReadQuorum))
	w.Header().Set(xhttp.MinIOStorageClassDefaults, strconv.FormatBool(result.UsingDefaults))
	// return how many drives are being healed if any
	if result.HealingDrives > 0 {
		w.Header().Set(xhttp.MinIOHealingDrives, strconv.Itoa(result.HealingDrives))
	}
	if !result.HealthyRead {
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

// ReadinessCheckHandler checks whether MinIO is up and ready to serve requests.
// It also checks whether the KMS is available and whether etcd is reachable,
// if configured.
func ReadinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	if objLayer := newObjectLayerFn(); objLayer == nil {
		w.Header().Set(xhttp.MinIOServerStatus, unavailable) // Service not initialized yet
	}
	if r.Header.Get(xhttp.MinIOPeerCall) != "" {
		writeResponse(w, http.StatusOK, nil, mimeNone)
		return
	}

	if int(globalHTTPStats.loadRequestsInQueue()) > globalAPIConfig.getRequestsPoolCapacity() {
		apiErr := getAPIError(ErrBusy)
		switch r.Method {
		case http.MethodHead:
			writeResponse(w, apiErr.HTTPStatusCode, nil, mimeNone)
		case http.MethodGet:
			writeErrorResponse(r.Context(), w, apiErr, r.URL)
		}
		return
	}

	// Verify if KMS is reachable if its configured
	if GlobalKMS != nil {
		ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
		defer cancel()

		if _, err := GlobalKMS.GenerateKey(ctx, &kms.GenerateKeyRequest{AssociatedData: kms.Context{"healthcheck": ""}}); err != nil {
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
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// LivenessCheckHandler checks whether MinIO is up. It differs from the
// readiness handler since a failing liveness check causes pod restarts
// in K8S environments. Therefore, it does not contact external systems.
func LivenessCheckHandler(w http.ResponseWriter, r *http.Request) {
	if objLayer := newObjectLayerFn(); objLayer == nil {
		w.Header().Set(xhttp.MinIOServerStatus, unavailable) // Service not initialized yet
	}
	if r.Header.Get(xhttp.MinIOPeerCall) != "" {
		writeResponse(w, http.StatusOK, nil, mimeNone)
		return
	}

	if int(globalHTTPStats.loadRequestsInQueue()) > globalAPIConfig.getRequestsPoolCapacity() {
		apiErr := getAPIError(ErrBusy)
		switch r.Method {
		case http.MethodHead:
			writeResponse(w, apiErr.HTTPStatusCode, nil, mimeNone)
		case http.MethodGet:
			writeErrorResponse(r.Context(), w, apiErr, r.URL)
		}
		return
	}
	writeResponse(w, http.StatusOK, nil, mimeNone)
}
