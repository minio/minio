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

// IdentityManager is the generic interface that handles KMS identity operations
type IdentityManager interface {
	// DescribeIdentity describes an identity by returning its metadata.
	// e.g. which policy is currently assigned and whether its an admin identity.
	DescribeIdentity(ctx context.Context, identity string) (*kes.IdentityInfo, error)

	// DescribeSelfIdentity describes the identity issuing the request.
	// It infers the identity from the TLS client certificate used to authenticate.
	// It returns the identity and policy information for the client identity.
	DescribeSelfIdentity(ctx context.Context) (*kes.IdentityInfo, *kes.Policy, error)

	// 	DeleteIdentity deletes an identity from KMS.
	// The client certificate that corresponds to the identity is no longer authorized to perform any API operations.
	// The admin identity cannot be deleted.
	DeleteIdentity(ctx context.Context, identity string) error

	// ListIdentities list all identity metadata that match the specified pattern.
	// In particular, the pattern * lists all identity metadata.
	ListIdentities(ctx context.Context, pattern string) (*kes.IdentityIterator, error)
}
