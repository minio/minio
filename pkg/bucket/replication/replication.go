/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package replication

import (
	"encoding/xml"
	"io"
	"sort"
	"strconv"
	"strings"
)

// StatusType of Replication for x-amz-replication-status header
type StatusType string

const (
	// Pending - replication is pending.
	Pending StatusType = "PENDING"

	// Complete - replication completed ok.
	Complete StatusType = "COMPLETE"

	// Failed - replication failed.
	Failed StatusType = "FAILED"

	// Replica - this is a replica.
	Replica StatusType = "REPLICA"
)

// String returns string representation of status
func (s StatusType) String() string {
	return string(s)
}

var (
	errReplicationTooManyRules        = Errorf("Replication configuration allows a maximum of 1000 rules")
	errReplicationNoRule              = Errorf("Replication configuration should have at least one rule")
	errReplicationUniquePriority      = Errorf("Replication configuration has duplicate priority")
	errReplicationDestinationMismatch = Errorf("The destination bucket must be same for all rules")
	errRoleArnMissing                 = Errorf("Missing required parameter `Role` in ReplicationConfiguration")
)

// Config - replication configuration specified in
// https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html
type Config struct {
	XMLName xml.Name `xml:"ReplicationConfiguration" json:"-"`
	Rules   []Rule   `xml:"Rule" json:"Rules"`
	// RoleArn is being reused for MinIO replication ARN
	RoleArn string `xml:"Role" json:"Role"`
}

// Maximum 2MiB size per replication config.
const maxReplicationConfigSize = 2 << 20

// ParseConfig parses ReplicationConfiguration from xml
func ParseConfig(reader io.Reader) (*Config, error) {
	config := Config{}
	if err := xml.NewDecoder(io.LimitReader(reader, maxReplicationConfigSize)).Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

// Validate - validates the replication configuration
func (c Config) Validate(bucket string, sameTarget bool) error {
	// replication config can't have more than 1000 rules
	if len(c.Rules) > 1000 {
		return errReplicationTooManyRules
	}
	// replication config should have at least one rule
	if len(c.Rules) == 0 {
		return errReplicationNoRule
	}
	if c.RoleArn == "" {
		return errRoleArnMissing
	}
	// Validate all the rules in the replication config
	targetMap := make(map[string]struct{})
	priorityMap := make(map[string]struct{})
	for _, r := range c.Rules {
		if len(targetMap) == 0 {
			targetMap[r.Destination.Bucket] = struct{}{}
		}
		if _, ok := targetMap[r.Destination.Bucket]; !ok {
			return errReplicationDestinationMismatch
		}
		if err := r.Validate(bucket, sameTarget); err != nil {
			return err
		}
		if _, ok := priorityMap[strconv.Itoa(r.Priority)]; ok {
			return errReplicationUniquePriority
		}
		priorityMap[strconv.Itoa(r.Priority)] = struct{}{}
	}
	return nil
}

// ObjectOpts provides information to deduce whether replication
// can be triggered on the resultant object.
type ObjectOpts struct {
	Name         string
	UserTags     string
	VersionID    string
	IsLatest     bool
	DeleteMarker bool
	SSEC         bool
}

// FilterActionableRules returns the rules actions that need to be executed
// after evaluating prefix/tag filtering
func (c Config) FilterActionableRules(obj ObjectOpts) []Rule {
	if obj.Name == "" {
		return nil
	}
	var rules []Rule
	for _, rule := range c.Rules {
		if rule.Status == Disabled {
			continue
		}
		if !strings.HasPrefix(obj.Name, rule.Prefix()) {
			continue
		}
		if rule.Filter.TestTags(strings.Split(obj.UserTags, "&")) {
			rules = append(rules, rule)
		}
	}
	sort.Slice(rules[:], func(i, j int) bool {
		return rules[i].Priority > rules[j].Priority
	})
	return rules
}

// GetDestination returns destination bucket and storage class.
func (c Config) GetDestination() Destination {
	if len(c.Rules) > 0 {
		return c.Rules[0].Destination
	}
	return Destination{}
}

// Replicate returns true if the object should be replicated.
func (c Config) Replicate(obj ObjectOpts) bool {

	for _, rule := range c.FilterActionableRules(obj) {

		if obj.DeleteMarker {
			// Indicates whether MinIO will remove a delete marker. By default, delete markers
			// are not replicated.
			return false
		}
		if obj.SSEC {
			return false
		}
		if obj.VersionID != "" && !obj.IsLatest {
			return false
		}
		if rule.Status == Disabled {
			continue
		}
		return true
	}
	return false
}

// HasActiveRules - returns whether replication policy has active rules
// Optionally a prefix can be supplied.
// If recursive is specified the function will also return true if any level below the
// prefix has active rules. If no prefix is specified recursive is effectively true.
func (c Config) HasActiveRules(prefix string, recursive bool) bool {
	if len(c.Rules) == 0 {
		return false
	}
	for _, rule := range c.Rules {
		if rule.Status == Disabled {
			continue
		}
		if len(prefix) > 0 && len(rule.Filter.Prefix) > 0 {
			// incoming prefix must be in rule prefix
			if !recursive && !strings.HasPrefix(prefix, rule.Filter.Prefix) {
				continue
			}
			// If recursive, we can skip this rule if it doesn't match the tested prefix.
			if recursive && !strings.HasPrefix(rule.Filter.Prefix, prefix) {
				continue
			}
		}
		return true
	}
	return false
}
