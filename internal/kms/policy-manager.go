// Copyright (c) 2015-2022 MinIO, Inc.
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

package kms

import (
	"context"

	"github.com/minio/kes"
)

// PolicyManager is the generic interface that handles KMS policy] operations
type PolicyManager interface {
	// DescribePolicy describes a policy by returning its metadata.
	// e.g. who created the policy at which point in time.
	DescribePolicy(ctx context.Context, policy string) (*kes.PolicyInfo, error)

	// AssignPolicy assigns a policy to an identity.
	// An identity can have at most one policy while the same policy can be assigned to multiple identities.
	// The assigned policy defines which API calls this identity can perform.
	// It's not possible to assign a policy to the admin identity.
	// Further, an identity cannot assign a policy to itself.
	AssignPolicy(ctx context.Context, policy, identity string) error

	// SetPolicy creates or updates a policy.
	SetPolicy(ctx context.Context, policy string, policyItem *kes.Policy) error

	// GetPolicy gets a policy from KMS.
	GetPolicy(ctx context.Context, policy string) (*kes.Policy, error)

	// ListPolicies list all policy metadata that match the specified pattern.
	// In particular, the pattern * lists all policy metadata.
	ListPolicies(ctx context.Context, pattern string) (*kes.PolicyIterator, error)

	// DeletePolicy	deletes a policy from KMS.
	// All identities that have been assigned to this policy will lose all authorization privileges.
	DeletePolicy(ctx context.Context, policy string) error
}
