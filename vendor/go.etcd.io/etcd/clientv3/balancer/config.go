// Copyright 2018 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package balancer

import (
	"go.etcd.io/etcd/clientv3/balancer/picker"

	"go.uber.org/zap"
)

// Config defines balancer configurations.
type Config struct {
	// Policy configures balancer policy.
	Policy picker.Policy

	// Name defines an additional name for balancer.
	// Useful for balancer testing to avoid register conflicts.
	// If empty, defaults to policy name.
	Name string

	// Logger configures balancer logging.
	// If nil, logs are discarded.
	Logger *zap.Logger
}
