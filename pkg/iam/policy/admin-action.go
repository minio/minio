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

package iampolicy

import (
	"github.com/minio/minio/pkg/bucket/policy/condition"
)

// AdminAction - admin policy action.
type AdminAction string

const (
	// HealAdminAction - allows heal command
	HealAdminAction = "admin:Heal"

	// Service Actions

	// StorageInfoAdminAction - allow listing server info
	StorageInfoAdminAction = "admin:StorageInfo"
	// PrometheusAdminAction - prometheus info action
	PrometheusAdminAction = "admin:Prometheus"
	// DataUsageInfoAdminAction - allow listing data usage info
	DataUsageInfoAdminAction = "admin:DataUsageInfo"
	// ForceUnlockAdminAction - allow force unlocking locks
	ForceUnlockAdminAction = "admin:ForceUnlock"
	// TopLocksAdminAction - allow listing top locks
	TopLocksAdminAction = "admin:TopLocksInfo"
	// ProfilingAdminAction - allow profiling
	ProfilingAdminAction = "admin:Profiling"
	// TraceAdminAction - allow listing server trace
	TraceAdminAction = "admin:ServerTrace"
	// ConsoleLogAdminAction - allow listing console logs on terminal
	ConsoleLogAdminAction = "admin:ConsoleLog"
	// KMSCreateKeyAdminAction - allow creating a new KMS master key
	KMSCreateKeyAdminAction = "admin:KMSCreateKey"
	// KMSKeyStatusAdminAction - allow getting KMS key status
	KMSKeyStatusAdminAction = "admin:KMSKeyStatus"
	// ServerInfoAdminAction - allow listing server info
	ServerInfoAdminAction = "admin:ServerInfo"
	// HealthInfoAdminAction - allow obtaining cluster health information
	HealthInfoAdminAction = "admin:OBDInfo"
	// BandwidthMonitorAction - allow monitoring bandwidth usage
	BandwidthMonitorAction = "admin:BandwidthMonitor"

	// ServerUpdateAdminAction - allow MinIO binary update
	ServerUpdateAdminAction = "admin:ServerUpdate"
	// ServiceRestartAdminAction - allow restart of MinIO service.
	ServiceRestartAdminAction = "admin:ServiceRestart"
	// ServiceStopAdminAction - allow stopping MinIO service.
	ServiceStopAdminAction = "admin:ServiceStop"

	// ConfigUpdateAdminAction - allow MinIO config management
	ConfigUpdateAdminAction = "admin:ConfigUpdate"

	// CreateUserAdminAction - allow creating MinIO user
	CreateUserAdminAction = "admin:CreateUser"
	// DeleteUserAdminAction - allow deleting MinIO user
	DeleteUserAdminAction = "admin:DeleteUser"
	// ListUsersAdminAction - allow list users permission
	ListUsersAdminAction = "admin:ListUsers"
	// EnableUserAdminAction - allow enable user permission
	EnableUserAdminAction = "admin:EnableUser"
	// DisableUserAdminAction - allow disable user permission
	DisableUserAdminAction = "admin:DisableUser"
	// GetUserAdminAction - allows GET permission on user info
	GetUserAdminAction = "admin:GetUser"

	// Service account Actions

	// CreateServiceAccountAdminAction - allow create a service account for a user
	CreateServiceAccountAdminAction = "admin:CreateServiceAccount"
	// UpdateServiceAccountAdminAction - allow updating a service account
	UpdateServiceAccountAdminAction = "admin:UpdateServiceAccount"
	// RemoveServiceAccountAdminAction - allow removing a service account
	RemoveServiceAccountAdminAction = "admin:RemoveServiceAccount"
	// ListServiceAccountsAdminAction - allow listing service accounts
	ListServiceAccountsAdminAction = "admin:ListServiceAccounts"

	// Group Actions

	// AddUserToGroupAdminAction - allow adding user to group permission
	AddUserToGroupAdminAction = "admin:AddUserToGroup"
	// RemoveUserFromGroupAdminAction - allow removing user to group permission
	RemoveUserFromGroupAdminAction = "admin:RemoveUserFromGroup"
	// GetGroupAdminAction - allow getting group info
	GetGroupAdminAction = "admin:GetGroup"
	// ListGroupsAdminAction - allow list groups permission
	ListGroupsAdminAction = "admin:ListGroups"
	// EnableGroupAdminAction - allow enable group permission
	EnableGroupAdminAction = "admin:EnableGroup"
	// DisableGroupAdminAction - allow disable group permission
	DisableGroupAdminAction = "admin:DisableGroup"

	// Policy Actions

	// CreatePolicyAdminAction - allow create policy permission
	CreatePolicyAdminAction = "admin:CreatePolicy"
	// DeletePolicyAdminAction - allow delete policy permission
	DeletePolicyAdminAction = "admin:DeletePolicy"
	// GetPolicyAdminAction - allow get policy permission
	GetPolicyAdminAction = "admin:GetPolicy"
	// AttachPolicyAdminAction - allows attaching a policy to a user/group
	AttachPolicyAdminAction = "admin:AttachUserOrGroupPolicy"
	// ListUserPoliciesAdminAction - allows listing user policies
	ListUserPoliciesAdminAction = "admin:ListUserPolicies"

	// Bucket quota Actions

	// SetBucketQuotaAdminAction - allow setting bucket quota
	SetBucketQuotaAdminAction = "admin:SetBucketQuota"
	// GetBucketQuotaAdminAction - allow getting bucket quota
	GetBucketQuotaAdminAction = "admin:GetBucketQuota"

	// Bucket Target admin Actions

	// SetBucketTargetAction - allow setting bucket target
	SetBucketTargetAction = "admin:SetBucketTarget"
	// GetBucketTargetAction - allow getting bucket targets
	GetBucketTargetAction = "admin:GetBucketTarget"

	// Remote Tier admin Actions

	// SetTierAction - allow adding/editing a remote tier
	SetTierAction = "admin:SetTier"
	// ListTierAction - allow listing remote tiers
	ListTierAction = "admin:ListTier"

	// AllAdminActions - provides all admin permissions
	AllAdminActions = "admin:*"
)

// List of all supported admin actions.
var supportedAdminActions = map[AdminAction]struct{}{
	HealAdminAction:                 {},
	StorageInfoAdminAction:          {},
	DataUsageInfoAdminAction:        {},
	TopLocksAdminAction:             {},
	ProfilingAdminAction:            {},
	PrometheusAdminAction:           {},
	TraceAdminAction:                {},
	ConsoleLogAdminAction:           {},
	KMSKeyStatusAdminAction:         {},
	ServerInfoAdminAction:           {},
	HealthInfoAdminAction:           {},
	BandwidthMonitorAction:          {},
	ServerUpdateAdminAction:         {},
	ServiceRestartAdminAction:       {},
	ServiceStopAdminAction:          {},
	ConfigUpdateAdminAction:         {},
	CreateUserAdminAction:           {},
	DeleteUserAdminAction:           {},
	ListUsersAdminAction:            {},
	EnableUserAdminAction:           {},
	DisableUserAdminAction:          {},
	GetUserAdminAction:              {},
	AddUserToGroupAdminAction:       {},
	RemoveUserFromGroupAdminAction:  {},
	GetGroupAdminAction:             {},
	ListGroupsAdminAction:           {},
	EnableGroupAdminAction:          {},
	DisableGroupAdminAction:         {},
	CreateServiceAccountAdminAction: {},
	UpdateServiceAccountAdminAction: {},
	RemoveServiceAccountAdminAction: {},
	ListServiceAccountsAdminAction:  {},
	CreatePolicyAdminAction:         {},
	DeletePolicyAdminAction:         {},
	GetPolicyAdminAction:            {},
	AttachPolicyAdminAction:         {},
	ListUserPoliciesAdminAction:     {},
	SetBucketQuotaAdminAction:       {},
	GetBucketQuotaAdminAction:       {},
	SetBucketTargetAction:           {},
	GetBucketTargetAction:           {},
	SetTierAction:                   {},
	ListTierAction:                  {},
	AllAdminActions:                 {},
}

// IsValid - checks if action is valid or not.
func (action AdminAction) IsValid() bool {
	_, ok := supportedAdminActions[action]
	return ok
}

// adminActionConditionKeyMap - holds mapping of supported condition key for an action.
var adminActionConditionKeyMap = map[Action]condition.KeySet{
	AllAdminActions:                 condition.NewKeySet(condition.AllSupportedAdminKeys...),
	HealAdminAction:                 condition.NewKeySet(condition.AllSupportedAdminKeys...),
	StorageInfoAdminAction:          condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ServerInfoAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	DataUsageInfoAdminAction:        condition.NewKeySet(condition.AllSupportedAdminKeys...),
	HealthInfoAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	BandwidthMonitorAction:          condition.NewKeySet(condition.AllSupportedAdminKeys...),
	TopLocksAdminAction:             condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ProfilingAdminAction:            condition.NewKeySet(condition.AllSupportedAdminKeys...),
	TraceAdminAction:                condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ConsoleLogAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	KMSKeyStatusAdminAction:         condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ServerUpdateAdminAction:         condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ServiceRestartAdminAction:       condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ServiceStopAdminAction:          condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ConfigUpdateAdminAction:         condition.NewKeySet(condition.AllSupportedAdminKeys...),
	CreateUserAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	DeleteUserAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ListUsersAdminAction:            condition.NewKeySet(condition.AllSupportedAdminKeys...),
	EnableUserAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	DisableUserAdminAction:          condition.NewKeySet(condition.AllSupportedAdminKeys...),
	GetUserAdminAction:              condition.NewKeySet(condition.AllSupportedAdminKeys...),
	AddUserToGroupAdminAction:       condition.NewKeySet(condition.AllSupportedAdminKeys...),
	RemoveUserFromGroupAdminAction:  condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ListGroupsAdminAction:           condition.NewKeySet(condition.AllSupportedAdminKeys...),
	EnableGroupAdminAction:          condition.NewKeySet(condition.AllSupportedAdminKeys...),
	DisableGroupAdminAction:         condition.NewKeySet(condition.AllSupportedAdminKeys...),
	CreateServiceAccountAdminAction: condition.NewKeySet(condition.AllSupportedAdminKeys...),
	UpdateServiceAccountAdminAction: condition.NewKeySet(condition.AllSupportedAdminKeys...),
	RemoveServiceAccountAdminAction: condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ListServiceAccountsAdminAction:  condition.NewKeySet(condition.AllSupportedAdminKeys...),

	CreatePolicyAdminAction:     condition.NewKeySet(condition.AllSupportedAdminKeys...),
	DeletePolicyAdminAction:     condition.NewKeySet(condition.AllSupportedAdminKeys...),
	GetPolicyAdminAction:        condition.NewKeySet(condition.AllSupportedAdminKeys...),
	AttachPolicyAdminAction:     condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ListUserPoliciesAdminAction: condition.NewKeySet(condition.AllSupportedAdminKeys...),
	SetBucketQuotaAdminAction:   condition.NewKeySet(condition.AllSupportedAdminKeys...),
	GetBucketQuotaAdminAction:   condition.NewKeySet(condition.AllSupportedAdminKeys...),
	SetBucketTargetAction:       condition.NewKeySet(condition.AllSupportedAdminKeys...),
	GetBucketTargetAction:       condition.NewKeySet(condition.AllSupportedAdminKeys...),
	SetTierAction:               condition.NewKeySet(condition.AllSupportedAdminKeys...),
	ListTierAction:              condition.NewKeySet(condition.AllSupportedAdminKeys...),
}
