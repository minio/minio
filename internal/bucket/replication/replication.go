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

package replication

import (
	"encoding/xml"
	"io"
	"sort"
	"strconv"
	"strings"
)

var (
	errReplicationTooManyRules          = Errorf("Replication configuration allows a maximum of 1000 rules")
	errReplicationNoRule                = Errorf("Replication configuration should have at least one rule")
	errReplicationUniquePriority        = Errorf("Replication configuration has duplicate priority")
	errRoleArnMissingLegacy             = Errorf("Missing required parameter `Role` in ReplicationConfiguration")
	errDestinationArnMissing            = Errorf("Missing required parameter `Destination` in Replication rule")
	errInvalidSourceSelectionCriteria   = Errorf("Invalid ReplicaModification status")
	errRoleArnPresentForMultipleTargets = Errorf("`Role` should be empty in ReplicationConfiguration for multiple targets")
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
	// By default, set replica modification to enabled if unset.
	for i := range config.Rules {
		if len(config.Rules[i].SourceSelectionCriteria.ReplicaModifications.Status) == 0 {
			config.Rules[i].SourceSelectionCriteria = SourceSelectionCriteria{
				ReplicaModifications: ReplicaModifications{
					Status: Enabled,
				},
			}
		}
		// Default DeleteReplication to disabled if unset.
		if len(config.Rules[i].DeleteReplication.Status) == 0 {
			config.Rules[i].DeleteReplication = DeleteReplication{
				Status: Disabled,
			}
		}
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

	// Validate all the rules in the replication config
	targetMap := make(map[string]struct{})
	priorityMap := make(map[string]struct{})
	var legacyArn bool
	for _, r := range c.Rules {
		if _, ok := targetMap[r.Destination.Bucket]; !ok {
			targetMap[r.Destination.Bucket] = struct{}{}
		}
		if err := r.Validate(bucket, sameTarget); err != nil {
			return err
		}
		if _, ok := priorityMap[strconv.Itoa(r.Priority)]; ok {
			return errReplicationUniquePriority
		}
		priorityMap[strconv.Itoa(r.Priority)] = struct{}{}

		if r.Destination.LegacyArn() {
			legacyArn = true
		}
		if c.RoleArn == "" && !r.Destination.TargetArn() {
			return errDestinationArnMissing
		}
	}
	// disallow combining old replication configuration which used RoleArn as target ARN with multiple
	// destination replication
	if c.RoleArn != "" && len(targetMap) > 1 {
		return errRoleArnPresentForMultipleTargets
	}
	// validate RoleArn if destination used legacy ARN format.
	if c.RoleArn == "" && legacyArn {
		return errRoleArnMissingLegacy
	}
	return nil
}

// Types of replication
const (
	UnsetReplicationType Type = 0 + iota
	ObjectReplicationType
	DeleteReplicationType
	MetadataReplicationType
	HealReplicationType
	ExistingObjectReplicationType
	ResyncReplicationType
	AllReplicationType
)

// Valid returns true if replication type is set
func (t Type) Valid() bool {
	return t > 0
}

// IsDataReplication returns true if content being replicated
func (t Type) IsDataReplication() bool {
	switch t {
	case ObjectReplicationType, HealReplicationType, ExistingObjectReplicationType:
		return true
	}
	return false
}

// ObjectOpts provides information to deduce whether replication
// can be triggered on the resultant object.
type ObjectOpts struct {
	Name           string
	UserTags       string
	VersionID      string
	DeleteMarker   bool
	SSEC           bool
	OpType         Type
	Replica        bool
	ExistingObject bool
	TargetArn      string
}

// HasExistingObjectReplication returns true if any of the rule returns 'ExistingObjects' replication.
func (c Config) HasExistingObjectReplication(arn string) (hasARN, isEnabled bool) {
	for _, rule := range c.Rules {
		if rule.Destination.ARN == arn || c.RoleArn == arn {
			if !hasARN {
				hasARN = true
			}
			if rule.ExistingObjectReplication.Status == Enabled {
				return true, true
			}
		}
	}
	return hasARN, false
}

// FilterActionableRules returns the rules actions that need to be executed
// after evaluating prefix/tag filtering
func (c Config) FilterActionableRules(obj ObjectOpts) []Rule {
	if obj.Name == "" && (obj.OpType != ResyncReplicationType && obj.OpType != AllReplicationType) {
		return nil
	}
	var rules []Rule
	for _, rule := range c.Rules {
		if rule.Status == Disabled {
			continue
		}

		if obj.TargetArn != "" && rule.Destination.ARN != obj.TargetArn && c.RoleArn != obj.TargetArn {
			continue
		}
		// Ignore other object level and prefix filters for resyncing target/listing bucket targets
		if obj.OpType == ResyncReplicationType || obj.OpType == AllReplicationType {
			rules = append(rules, rule)
			continue
		}
		if obj.ExistingObject && rule.ExistingObjectReplication.Status == Disabled {
			continue
		}
		if !strings.HasPrefix(obj.Name, rule.Prefix()) {
			continue
		}
		if rule.Filter.TestTags(obj.UserTags) {
			rules = append(rules, rule)
		}
	}
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Priority > rules[j].Priority && rules[i].Destination.String() == rules[j].Destination.String()
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
		if rule.Status == Disabled {
			continue
		}
		if obj.ExistingObject && rule.ExistingObjectReplication.Status == Disabled {
			return false
		}
		if obj.OpType == DeleteReplicationType {
			switch {
			case obj.VersionID != "":
				// check MinIO extension for versioned deletes
				return rule.DeleteReplication.Status == Enabled
			default:
				return rule.DeleteMarkerReplication.Status == Enabled
			}
		} // regular object/metadata replication
		return rule.MetadataReplicate(obj)
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
			// If recursive, we can skip this rule if it doesn't match the tested prefix or level below prefix
			// does not match
			if recursive && !strings.HasPrefix(rule.Prefix(), prefix) && !strings.HasPrefix(prefix, rule.Prefix()) {
				continue
			}
		}
		return true
	}
	return false
}

// FilterTargetArns returns a slice of distinct target arns in the config
func (c Config) FilterTargetArns(obj ObjectOpts) []string {
	var arns []string

	tgtsMap := make(map[string]struct{})
	rules := c.FilterActionableRules(obj)
	for _, rule := range rules {
		if rule.Status == Disabled {
			continue
		}
		if c.RoleArn != "" {
			arns = append(arns, c.RoleArn) // use legacy RoleArn if present
			return arns
		}
		if _, ok := tgtsMap[rule.Destination.ARN]; !ok {
			tgtsMap[rule.Destination.ARN] = struct{}{}
		}
	}
	for k := range tgtsMap {
		arns = append(arns, k)
	}
	return arns
}
