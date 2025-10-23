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
	"net/http"

	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/gzip"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
)

const (
	adminPathPrefix                = minioReservedBucketPath + "/admin"
	adminAPIVersion                = madmin.AdminAPIVersion
	adminAPIVersionPrefix          = SlashSeparator + adminAPIVersion
	adminAPISiteReplicationDevNull = "/site-replication/devnull"
	adminAPISiteReplicationNetPerf = "/site-replication/netperf"
	adminAPIClientDevNull          = "/speedtest/client/devnull"
	adminAPIClientDevExtraTime     = "/speedtest/client/devnull/extratime"
)

var gzipHandler = func() func(http.Handler) http.HandlerFunc {
	gz, err := gzhttp.NewWrapper(gzhttp.MinSize(1000), gzhttp.CompressionLevel(gzip.BestSpeed))
	if err != nil {
		// Static params, so this is very unlikely.
		logger.Fatal(err, "Unable to initialize server")
	}
	return gz
}()

// Set of handler options as bit flags
type hFlag uint8

const (
	// this flag disables gzip compression of responses
	noGZFlag = 1 << iota

	// this flag enables tracing body and headers instead of just headers
	traceAllFlag

	// pass this flag to skip checking if object layer is available
	noObjLayerFlag
)

// Has checks if the given flag is enabled in `h`.
func (h hFlag) Has(flag hFlag) bool {
	// Use bitwise-AND and check if the result is non-zero.
	return h&flag != 0
}

// adminMiddleware performs some common admin handler functionality for all
// handlers:
//
// - updates request context with `logger.ReqInfo` and api name based on the
// name of the function handler passed (this handler must be a method of
// `adminAPIHandlers`).
//
// - sets up call to send AuditLog
//
// While this is a middleware function (i.e. it takes a handler function and
// returns one), due to flags being passed based on required conditions, it is
// done per-"handler function registration" in the router.
//
// The passed in handler function must be a method of `adminAPIHandlers` for the
// name displayed in logs and trace to be accurate. The name is extracted via
// reflection.
//
// When no flags are passed, gzip compression, http tracing of headers and
// checking of object layer availability are all enabled. Use flags to modify
// this behavior.
func adminMiddleware(f http.HandlerFunc, flags ...hFlag) http.HandlerFunc {
	// Collect all flags with bitwise-OR and assign operator
	var handlerFlags hFlag
	for _, flag := range flags {
		handlerFlags |= flag
	}

	// Get name of the handler using reflection.
	handlerName := getHandlerName(f, "adminAPIHandlers")

	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		// Update request context with `logger.ReqInfo`.
		r = r.WithContext(newContext(r, w, handlerName))

		defer logger.AuditLog(r.Context(), w, r, mustGetClaimsFromToken(r))

		// Check if object layer is available, if not return error early.
		if !handlerFlags.Has(noObjLayerFlag) {
			objectAPI := newObjectLayerFn()
			if objectAPI == nil || globalNotificationSys == nil {
				writeErrorResponseJSON(r.Context(), w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
				return
			}
		}

		// Apply http tracing "middleware" based on presence of flag.
		var f2 http.HandlerFunc
		if handlerFlags.Has(traceAllFlag) {
			f2 = httpTraceAll(f)
		} else {
			f2 = httpTraceHdrs(f)
		}

		// call the final handler
		f2(w, r)
	}

	// Enable compression of responses based on presence of flag.
	if !handlerFlags.Has(noGZFlag) {
		handler = gzipHandler(handler)
	}

	return handler
}

// adminAPIHandlers provides HTTP handlers for MinIO admin API.
type adminAPIHandlers struct{}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableConfigOps bool) {
	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminPathPrefix).Subrouter()

	adminVersions := []string{
		adminAPIVersionPrefix,
	}

	for _, adminVersion := range adminVersions {
		// Restart and stop MinIO service type=2
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/service").HandlerFunc(adminMiddleware(adminAPI.ServiceV2Handler, traceAllFlag)).Queries("action", "{action:.*}", "type", "2")

		// Deprecated: Restart and stop MinIO service.
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/service").HandlerFunc(adminMiddleware(adminAPI.ServiceHandler, traceAllFlag)).Queries("action", "{action:.*}")

		// Update all MinIO servers type=2
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update").HandlerFunc(adminMiddleware(adminAPI.ServerUpdateV2Handler, traceAllFlag)).Queries("updateURL", "{updateURL:.*}", "type", "2")

		// Deprecated: Update MinIO servers.
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update").HandlerFunc(adminMiddleware(adminAPI.ServerUpdateHandler, traceAllFlag)).Queries("updateURL", "{updateURL:.*}")

		// Info operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/info").HandlerFunc(adminMiddleware(adminAPI.ServerInfoHandler, traceAllFlag, noObjLayerFlag))
		adminRouter.Methods(http.MethodGet, http.MethodPost).Path(adminVersion + "/inspect-data").HandlerFunc(adminMiddleware(adminAPI.InspectDataHandler, noGZFlag, traceHdrsS3HFlag))

		// StorageInfo operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/storageinfo").HandlerFunc(adminMiddleware(adminAPI.StorageInfoHandler, traceAllFlag))
		// DataUsageInfo operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/datausageinfo").HandlerFunc(adminMiddleware(adminAPI.DataUsageInfoHandler, traceAllFlag))
		// Metrics operation
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/metrics").HandlerFunc(adminMiddleware(adminAPI.MetricsHandler, traceHdrsS3HFlag))

		if globalIsDistErasure || globalIsErasure {
			// Heal operations

			// Heal processing endpoint.
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/").HandlerFunc(adminMiddleware(adminAPI.HealHandler, traceAllFlag))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/{bucket}").HandlerFunc(adminMiddleware(adminAPI.HealHandler, traceAllFlag))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/{bucket}/{prefix:.*}").HandlerFunc(adminMiddleware(adminAPI.HealHandler, traceAllFlag))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/background-heal/status").HandlerFunc(adminMiddleware(adminAPI.BackgroundHealStatusHandler, traceAllFlag))

			// Pool operations
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/pools/list").HandlerFunc(adminMiddleware(adminAPI.ListPools, traceAllFlag))
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/pools/status").HandlerFunc(adminMiddleware(adminAPI.StatusPool, traceAllFlag)).Queries("pool", "{pool:.*}")

			adminRouter.Methods(http.MethodPost).Path(adminVersion+"/pools/decommission").HandlerFunc(adminMiddleware(adminAPI.StartDecommission, traceAllFlag)).Queries("pool", "{pool:.*}")
			adminRouter.Methods(http.MethodPost).Path(adminVersion+"/pools/cancel").HandlerFunc(adminMiddleware(adminAPI.CancelDecommission, traceAllFlag)).Queries("pool", "{pool:.*}")

			// Rebalance operations
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/rebalance/start").HandlerFunc(adminMiddleware(adminAPI.RebalanceStart, traceAllFlag))
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/rebalance/status").HandlerFunc(adminMiddleware(adminAPI.RebalanceStatus, traceAllFlag))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/rebalance/stop").HandlerFunc(adminMiddleware(adminAPI.RebalanceStop, traceAllFlag))
		}

		// Profiling operations - deprecated API
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/profiling/start").HandlerFunc(adminMiddleware(adminAPI.StartProfilingHandler, traceAllFlag, noObjLayerFlag)).
			Queries("profilerType", "{profilerType:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/profiling/download").HandlerFunc(adminMiddleware(adminAPI.DownloadProfilingHandler, traceHdrsS3HFlag, noObjLayerFlag))
		// Profiling operations
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/profile").HandlerFunc(adminMiddleware(adminAPI.ProfileHandler, traceHdrsS3HFlag, noObjLayerFlag))

		// Config KV operations.
		if enableConfigOps {
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-config-kv").HandlerFunc(adminMiddleware(adminAPI.GetConfigKVHandler)).Queries("key", "{key:.*}")
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/set-config-kv").HandlerFunc(adminMiddleware(adminAPI.SetConfigKVHandler))
			adminRouter.Methods(http.MethodDelete).Path(adminVersion + "/del-config-kv").HandlerFunc(adminMiddleware(adminAPI.DelConfigKVHandler))
		}

		// Enable config help in all modes.
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/help-config-kv").HandlerFunc(adminMiddleware(adminAPI.HelpConfigKVHandler, traceAllFlag)).Queries("subSys", "{subSys:.*}", "key", "{key:.*}")

		// Config KV history operations.
		if enableConfigOps {
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-config-history-kv").HandlerFunc(adminMiddleware(adminAPI.ListConfigHistoryKVHandler, traceAllFlag)).Queries("count", "{count:[0-9]+}")
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/clear-config-history-kv").HandlerFunc(adminMiddleware(adminAPI.ClearConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/restore-config-history-kv").HandlerFunc(adminMiddleware(adminAPI.RestoreConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
		}

		// Config import/export bulk operations
		if enableConfigOps {
			// Get config
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/config").HandlerFunc(adminMiddleware(adminAPI.GetConfigHandler))
			// Set config
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/config").HandlerFunc(adminMiddleware(adminAPI.SetConfigHandler))
		}

		// -- IAM APIs --

		// Add policy IAM
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-canned-policy").HandlerFunc(adminMiddleware(adminAPI.AddCannedPolicy, traceAllFlag)).Queries("name", "{name:.*}")

		// Add user IAM
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/accountinfo").HandlerFunc(adminMiddleware(adminAPI.AccountInfoHandler, traceAllFlag))

		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-user").HandlerFunc(adminMiddleware(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")

		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-status").HandlerFunc(adminMiddleware(adminAPI.SetUserStatus)).Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

		// Service accounts ops
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/add-service-account").HandlerFunc(adminMiddleware(adminAPI.AddServiceAccount))
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update-service-account").HandlerFunc(adminMiddleware(adminAPI.UpdateServiceAccount)).Queries("accessKey", "{accessKey:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-service-account").HandlerFunc(adminMiddleware(adminAPI.InfoServiceAccount)).Queries("accessKey", "{accessKey:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-service-accounts").HandlerFunc(adminMiddleware(adminAPI.ListServiceAccounts))
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/delete-service-account").HandlerFunc(adminMiddleware(adminAPI.DeleteServiceAccount)).Queries("accessKey", "{accessKey:.*}")

		// STS accounts ops
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/temporary-account-info").HandlerFunc(adminMiddleware(adminAPI.TemporaryAccountInfo)).Queries("accessKey", "{accessKey:.*}")

		// Access key (service account/STS) operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-access-keys-bulk").HandlerFunc(adminMiddleware(adminAPI.ListAccessKeysBulk)).Queries("listType", "{listType:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-access-key").HandlerFunc(adminMiddleware(adminAPI.InfoAccessKey)).Queries("accessKey", "{accessKey:.*}")

		// Info policy IAM latest
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-canned-policy").HandlerFunc(adminMiddleware(adminAPI.InfoCannedPolicy)).Queries("name", "{name:.*}")
		// List policies latest
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-canned-policies").HandlerFunc(adminMiddleware(adminAPI.ListBucketPolicies)).Queries("bucket", "{bucket:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-canned-policies").HandlerFunc(adminMiddleware(adminAPI.ListCannedPolicies))

		// Builtin IAM policy associations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/idp/builtin/policy-entities").HandlerFunc(adminMiddleware(adminAPI.ListPolicyMappingEntities))

		// Remove policy IAM
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-canned-policy").HandlerFunc(adminMiddleware(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

		// Set user or group policy
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-or-group-policy").
			HandlerFunc(adminMiddleware(adminAPI.SetPolicyForUserOrGroup)).
			Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}")

		// Attach/Detach policies to/from user or group
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/idp/builtin/policy/{operation}").HandlerFunc(adminMiddleware(adminAPI.AttachDetachPolicyBuiltin))

		// Remove user IAM
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-user").HandlerFunc(adminMiddleware(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

		// List users
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-users").HandlerFunc(adminMiddleware(adminAPI.ListBucketUsers)).Queries("bucket", "{bucket:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-users").HandlerFunc(adminMiddleware(adminAPI.ListUsers))

		// User info
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/user-info").HandlerFunc(adminMiddleware(adminAPI.GetUserInfo)).Queries("accessKey", "{accessKey:.*}")
		// Add/Remove members from group
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/update-group-members").HandlerFunc(adminMiddleware(adminAPI.UpdateGroupMembers))

		// Get Group
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/group").HandlerFunc(adminMiddleware(adminAPI.GetGroup)).Queries("group", "{group:.*}")

		// List Groups
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/groups").HandlerFunc(adminMiddleware(adminAPI.ListGroups))

		// Set Group Status
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-group-status").HandlerFunc(adminMiddleware(adminAPI.SetGroupStatus)).Queries("group", "{group:.*}").Queries("status", "{status:.*}")

		// Export IAM info to zipped file
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/export-iam").HandlerFunc(adminMiddleware(adminAPI.ExportIAM, noGZFlag))

		// Import IAM info
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/import-iam").HandlerFunc(adminMiddleware(adminAPI.ImportIAM, noGZFlag))
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/import-iam-v2").HandlerFunc(adminMiddleware(adminAPI.ImportIAMV2, noGZFlag))

		// Identity Provider configuration APIs
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/idp-config/{type}/{name}").HandlerFunc(adminMiddleware(adminAPI.AddIdentityProviderCfg))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/idp-config/{type}/{name}").HandlerFunc(adminMiddleware(adminAPI.UpdateIdentityProviderCfg))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/idp-config/{type}").HandlerFunc(adminMiddleware(adminAPI.ListIdentityProviderCfg))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/idp-config/{type}/{name}").HandlerFunc(adminMiddleware(adminAPI.GetIdentityProviderCfg))
		adminRouter.Methods(http.MethodDelete).Path(adminVersion + "/idp-config/{type}/{name}").HandlerFunc(adminMiddleware(adminAPI.DeleteIdentityProviderCfg))

		// LDAP specific service accounts ops
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/idp/ldap/add-service-account").HandlerFunc(adminMiddleware(adminAPI.AddServiceAccountLDAP))
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/idp/ldap/list-access-keys").
			HandlerFunc(adminMiddleware(adminAPI.ListAccessKeysLDAP)).Queries("userDN", "{userDN:.*}", "listType", "{listType:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/idp/ldap/list-access-keys-bulk").
			HandlerFunc(adminMiddleware(adminAPI.ListAccessKeysLDAPBulk)).Queries("listType", "{listType:.*}")

		// LDAP IAM operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/idp/ldap/policy-entities").HandlerFunc(adminMiddleware(adminAPI.ListLDAPPolicyMappingEntities))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/idp/ldap/policy/{operation}").HandlerFunc(adminMiddleware(adminAPI.AttachDetachPolicyLDAP))

		// OpenID specific service accounts ops
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/idp/openid/list-access-keys-bulk").
			HandlerFunc(adminMiddleware(adminAPI.ListAccessKeysOpenIDBulk)).Queries("listType", "{listType:.*}")

		// -- END IAM APIs --

		// GetBucketQuotaConfig
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-bucket-quota").HandlerFunc(
			adminMiddleware(adminAPI.GetBucketQuotaConfigHandler)).Queries("bucket", "{bucket:.*}")
		// PutBucketQuotaConfig
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-bucket-quota").HandlerFunc(
			adminMiddleware(adminAPI.PutBucketQuotaConfigHandler)).Queries("bucket", "{bucket:.*}")

		// Bucket replication operations
		// GetBucketTargetHandler
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-remote-targets").HandlerFunc(
			adminMiddleware(adminAPI.ListRemoteTargetsHandler)).Queries("bucket", "{bucket:.*}", "type", "{type:.*}")
		// SetRemoteTargetHandler
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-remote-target").HandlerFunc(
			adminMiddleware(adminAPI.SetRemoteTargetHandler)).Queries("bucket", "{bucket:.*}")
		// RemoveRemoteTargetHandler
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-remote-target").HandlerFunc(
			adminMiddleware(adminAPI.RemoveRemoteTargetHandler)).Queries("bucket", "{bucket:.*}", "arn", "{arn:.*}")
		// ReplicationDiff - MinIO extension API
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/replication/diff").HandlerFunc(
			adminMiddleware(adminAPI.ReplicationDiffHandler)).Queries("bucket", "{bucket:.*}")
		// ReplicationMRFHandler - MinIO extension API
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/replication/mrf").HandlerFunc(
			adminMiddleware(adminAPI.ReplicationMRFHandler)).Queries("bucket", "{bucket:.*}")

		// Batch job operations
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/start-job").HandlerFunc(
			adminMiddleware(adminAPI.StartBatchJob))

		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-jobs").HandlerFunc(
			adminMiddleware(adminAPI.ListBatchJobs))

		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/status-job").HandlerFunc(
			adminMiddleware(adminAPI.BatchJobStatus))

		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/describe-job").HandlerFunc(
			adminMiddleware(adminAPI.DescribeBatchJob))
		adminRouter.Methods(http.MethodDelete).Path(adminVersion + "/cancel-job").HandlerFunc(
			adminMiddleware(adminAPI.CancelBatchJob))

		// Bucket migration operations
		// ExportBucketMetaHandler
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/export-bucket-metadata").HandlerFunc(
			adminMiddleware(adminAPI.ExportBucketMetadataHandler))
		// ImportBucketMetaHandler
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/import-bucket-metadata").HandlerFunc(
			adminMiddleware(adminAPI.ImportBucketMetadataHandler))

		// Remote Tier management operations
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/tier").HandlerFunc(adminMiddleware(adminAPI.AddTierHandler))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/tier/{tier}").HandlerFunc(adminMiddleware(adminAPI.EditTierHandler))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/tier").HandlerFunc(adminMiddleware(adminAPI.ListTierHandler))
		adminRouter.Methods(http.MethodDelete).Path(adminVersion + "/tier/{tier}").HandlerFunc(adminMiddleware(adminAPI.RemoveTierHandler))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/tier/{tier}").HandlerFunc(adminMiddleware(adminAPI.VerifyTierHandler))
		// Tier stats
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/tier-stats").HandlerFunc(adminMiddleware(adminAPI.TierStatsHandler))

		// Cluster Replication APIs
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/add").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationAdd))
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/remove").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationRemove))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/site-replication/info").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationInfo))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/site-replication/metainfo").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationMetaInfo))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/site-replication/status").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationStatus))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + adminAPISiteReplicationDevNull).HandlerFunc(adminMiddleware(adminAPI.SiteReplicationDevNull, noObjLayerFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + adminAPISiteReplicationNetPerf).HandlerFunc(adminMiddleware(adminAPI.SiteReplicationNetPerf, noObjLayerFlag))

		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/peer/join").HandlerFunc(adminMiddleware(adminAPI.SRPeerJoin))
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/site-replication/peer/bucket-ops").HandlerFunc(adminMiddleware(adminAPI.SRPeerBucketOps)).Queries("bucket", "{bucket:.*}").Queries("operation", "{operation:.*}")
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/peer/iam-item").HandlerFunc(adminMiddleware(adminAPI.SRPeerReplicateIAMItem))
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/peer/bucket-meta").HandlerFunc(adminMiddleware(adminAPI.SRPeerReplicateBucketItem))
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/site-replication/peer/idp-settings").HandlerFunc(adminMiddleware(adminAPI.SRPeerGetIDPSettings))
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/edit").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationEdit))
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/peer/edit").HandlerFunc(adminMiddleware(adminAPI.SRPeerEdit))
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/peer/remove").HandlerFunc(adminMiddleware(adminAPI.SRPeerRemove))
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/site-replication/resync/op").HandlerFunc(adminMiddleware(adminAPI.SiteReplicationResyncOp)).Queries("operation", "{operation:.*}")
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/site-replication/state/edit").HandlerFunc(adminMiddleware(adminAPI.SRStateEdit))

		if globalIsDistErasure {
			// Top locks
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/top/locks").HandlerFunc(adminMiddleware(adminAPI.TopLocksHandler))
			// Force unlocks paths
			adminRouter.Methods(http.MethodPost).Path(adminVersion+"/force-unlock").
				Queries("paths", "{paths:.*}").HandlerFunc(adminMiddleware(adminAPI.ForceUnlockHandler))
		}

		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/speedtest").HandlerFunc(adminMiddleware(adminAPI.ObjectSpeedTestHandler, noGZFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/speedtest/object").HandlerFunc(adminMiddleware(adminAPI.ObjectSpeedTestHandler, noGZFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/speedtest/drive").HandlerFunc(adminMiddleware(adminAPI.DriveSpeedtestHandler, noGZFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/speedtest/net").HandlerFunc(adminMiddleware(adminAPI.NetperfHandler, noGZFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/speedtest/site").HandlerFunc(adminMiddleware(adminAPI.SitePerfHandler, noGZFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + adminAPIClientDevNull).HandlerFunc(adminMiddleware(adminAPI.ClientDevNull, noGZFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion + adminAPIClientDevExtraTime).HandlerFunc(adminMiddleware(adminAPI.ClientDevNullExtraTime, noGZFlag))

		// HTTP Trace
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/trace").HandlerFunc(adminMiddleware(adminAPI.TraceHandler, noObjLayerFlag))

		// Console Logs
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/log").HandlerFunc(adminMiddleware(adminAPI.ConsoleLogHandler, traceAllFlag))

		// -- KMS APIs --
		//
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/kms/status").HandlerFunc(adminMiddleware(adminAPI.KMSStatusHandler, traceAllFlag))
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/kms/key/create").HandlerFunc(adminMiddleware(adminAPI.KMSCreateKeyHandler, traceAllFlag)).Queries("key-id", "{key-id:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/kms/key/status").HandlerFunc(adminMiddleware(adminAPI.KMSKeyStatusHandler, traceAllFlag))

		// Keep obdinfo for backward compatibility with mc
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/obdinfo").
			HandlerFunc(adminMiddleware(adminAPI.HealthInfoHandler))
		// -- Health API --
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/healthinfo").
			HandlerFunc(adminMiddleware(adminAPI.HealthInfoHandler))

		// STS Revocation
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/revoke-tokens/{userProvider}").HandlerFunc(adminMiddleware(adminAPI.RevokeTokens))
	}

	// If none of the routes match add default error handler routes
	adminRouter.NotFoundHandler = httpTraceAll(errorResponseHandler)
	adminRouter.MethodNotAllowedHandler = httpTraceAll(methodNotAllowedHandler("Admin"))
}
