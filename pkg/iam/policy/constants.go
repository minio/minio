/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package iampolicy

import (
	"github.com/minio/minio/pkg/bucket/policy"
)

// Policy claim constants
const (
	PolicyName        = "policy"
	SessionPolicyName = "sessionPolicy"
)

// ReadWrite - provides full access to all buckets and all objects
var ReadWrite = Policy{
	Version: DefaultVersion,
	Statements: []Statement{
		{
			SID:       policy.ID(""),
			Effect:    policy.Allow,
			Actions:   NewActionSet(AllActions),
			Resources: NewResourceSet(NewResource("*", "")),
		},
	},
}

// ReadOnly - read only.
var ReadOnly = Policy{
	Version: DefaultVersion,
	Statements: []Statement{
		{
			SID:       policy.ID(""),
			Effect:    policy.Allow,
			Actions:   NewActionSet(GetBucketLocationAction, GetObjectAction),
			Resources: NewResourceSet(NewResource("*", "")),
		},
	},
}

// WriteOnly - provides write access.
var WriteOnly = Policy{
	Version: DefaultVersion,
	Statements: []Statement{
		{
			SID:       policy.ID(""),
			Effect:    policy.Allow,
			Actions:   NewActionSet(PutObjectAction),
			Resources: NewResourceSet(NewResource("*", "")),
		},
	},
}

// AdminDiagnostics - provides admin diagnostics access.
var AdminDiagnostics = Policy{
	Version: DefaultVersion,
	Statements: []Statement{
		{
			SID:    policy.ID(""),
			Effect: policy.Allow,
			Actions: NewActionSet(ProfilingAdminAction,
				TraceAdminAction, ConsoleLogAdminAction,
				ServerInfoAdminAction, TopLocksAdminAction,
				OBDInfoAdminAction),
			Resources: NewResourceSet(NewResource("*", "")),
		},
	},
}
