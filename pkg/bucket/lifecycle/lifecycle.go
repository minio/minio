/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package lifecycle

import (
	"encoding/xml"
	"io"
	"strings"
	"time"
)

var (
	errLifecycleTooManyRules      = Errorf("Lifecycle configuration allows a maximum of 1000 rules")
	errLifecycleNoRule            = Errorf("Lifecycle configuration should have at least one rule")
	errLifecycleOverlappingPrefix = Errorf("Lifecycle configuration has rules with overlapping prefix")
)

// Action represents a delete action or other transition
// actions that will be implemented later.
type Action int

//go:generate stringer -type Action $GOFILE

const (
	// NoneAction means no action required after evaluting lifecycle rules
	NoneAction Action = iota
	// DeleteAction means the object needs to be removed after evaluting lifecycle rules
	DeleteAction
)

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// HasActiveRules - returns whether policy has active rules for.
// Optionally a prefix can be supplied.
// If recursive is specified the function will also return true if any level below the
// prefix has active rules. If no prefix is specified recursive is effectively true.
func (lc Lifecycle) HasActiveRules(prefix string, recursive bool) bool {
	if len(lc.Rules) == 0 {
		return false
	}
	for _, rule := range lc.Rules {
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

		if rule.NoncurrentVersionExpiration.NoncurrentDays > 0 {
			return true
		}
		if rule.NoncurrentVersionTransition.NoncurrentDays > 0 {
			return true
		}
		if rule.Expiration.IsNull() {
			continue
		}
		if !rule.Expiration.IsDateNull() && rule.Expiration.Date.After(time.Now()) {
			continue
		}
		return true
	}
	return false
}

// ParseLifecycleConfig - parses data in given reader to Lifecycle.
func ParseLifecycleConfig(reader io.Reader) (*Lifecycle, error) {
	var lc Lifecycle
	if err := xml.NewDecoder(reader).Decode(&lc); err != nil {
		return nil, err
	}
	return &lc, nil
}

// Validate - validates the lifecycle configuration
func (lc Lifecycle) Validate() error {
	// Lifecycle config can't have more than 1000 rules
	if len(lc.Rules) > 1000 {
		return errLifecycleTooManyRules
	}
	// Lifecycle config should have at least one rule
	if len(lc.Rules) == 0 {
		return errLifecycleNoRule
	}
	// Validate all the rules in the lifecycle config
	for _, r := range lc.Rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	// Compare every rule's prefix with every other rule's prefix
	for i := range lc.Rules {
		if i == len(lc.Rules)-1 {
			break
		}
		// N B Empty prefixes overlap with all prefixes
		otherRules := lc.Rules[i+1:]
		for _, otherRule := range otherRules {
			if strings.HasPrefix(lc.Rules[i].Prefix(), otherRule.Prefix()) ||
				strings.HasPrefix(otherRule.Prefix(), lc.Rules[i].Prefix()) {
				return errLifecycleOverlappingPrefix
			}
		}
	}
	return nil
}

// FilterActionableRules returns the rules actions that need to be executed
// after evaluating prefix/tag filtering
func (lc Lifecycle) FilterActionableRules(objName, objTags string) []Rule {
	if objName == "" {
		return nil
	}
	var rules []Rule
	for _, rule := range lc.Rules {
		if rule.Status == Disabled {
			continue
		}
		if !strings.HasPrefix(objName, rule.Prefix()) {
			continue
		}
		tags := strings.Split(objTags, "&")
		if rule.Filter.TestTags(tags) {
			rules = append(rules, rule)
		}
	}
	return rules
}

// ComputeAction returns the action to perform by evaluating all lifecycle rules
// against the object name and its modification time.
func (lc Lifecycle) ComputeAction(objName, objTags string, modTime time.Time) (action Action) {
	action = NoneAction
	if modTime.IsZero() {
		return
	}

	_, expiryTime := lc.PredictExpiryTime(objName, modTime, objTags)
	if !expiryTime.IsZero() && time.Now().After(expiryTime) {
		return DeleteAction
	}
	return
}

// expectedExpiryTime calculates the expiry date/time based on a object modtime.
// The expected expiry time is always a midnight time following the the object
// modification time plus the number of expiration days.
//   e.g. If the object modtime is `Thu May 21 13:42:50 GMT 2020` and the object should
//        expire in 1 day, then the expected expiry time is `Fri, 23 May 2020 00:00:00 GMT`
func expectedExpiryTime(modTime time.Time, days ExpirationDays) time.Time {
	t := modTime.UTC().Add(time.Duration(days+1) * 24 * time.Hour)
	return t.Truncate(24 * time.Hour)
}

// PredictExpiryTime returns the expiry date/time of a given object
// after evaluting the current lifecycle document.
func (lc Lifecycle) PredictExpiryTime(objName string, modTime time.Time, objTags string) (string, time.Time) {
	var finalExpiryDate time.Time
	var finalExpiryRuleID string

	// Iterate over all actionable rules and find the earliest
	// expiration date and its associated rule ID.
	for _, rule := range lc.FilterActionableRules(objName, objTags) {
		if !rule.Expiration.IsDateNull() {
			if finalExpiryDate.IsZero() || finalExpiryDate.After(rule.Expiration.Date.Time) {
				finalExpiryRuleID = rule.ID
				finalExpiryDate = rule.Expiration.Date.Time
			}
		}
		if !rule.Expiration.IsDaysNull() {
			expectedExpiry := expectedExpiryTime(modTime, rule.Expiration.Days)
			if finalExpiryDate.IsZero() || finalExpiryDate.After(expectedExpiry) {
				finalExpiryRuleID = rule.ID
				finalExpiryDate = expectedExpiry
			}
		}
	}
	return finalExpiryRuleID, finalExpiryDate
}
