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

package lifecycle

import (
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"time"
)

var (
	errLifecycleTooManyRules = Errorf("Lifecycle configuration allows a maximum of 1000 rules")
	errLifecycleNoRule       = Errorf("Lifecycle configuration should have at least one rule")
	errLifecycleDuplicateID  = Errorf("Lifecycle configuration has rule with the same ID. Rule ID must be unique.")
	errXMLNotWellFormed      = Errorf("The XML you provided was not well-formed or did not validate against our published schema")
)

const (
	// TransitionComplete marks completed transition
	TransitionComplete = "complete"
	// TransitionPending - transition is yet to be attempted
	TransitionPending = "pending"
)

// Action represents a delete action or other transition
// actions that will be implemented later.
type Action int

//go:generate stringer -type Action $GOFILE

const (
	// NoneAction means no action required after evaluating lifecycle rules
	NoneAction Action = iota
	// DeleteAction means the object needs to be removed after evaluating lifecycle rules
	DeleteAction
	// DeleteVersionAction deletes a particular version
	DeleteVersionAction
	// TransitionAction transitions a particular object after evaluating lifecycle transition rules
	TransitionAction
	//TransitionVersionAction transitions a particular object version after evaluating lifecycle transition rules
	TransitionVersionAction
	// DeleteRestoredAction means the temporarily restored object needs to be removed after evaluating lifecycle rules
	DeleteRestoredAction
	// DeleteRestoredVersionAction deletes a particular version that was temporarily restored
	DeleteRestoredVersionAction
)

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// UnmarshalXML - decodes XML data.
func (lc *Lifecycle) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	switch start.Name.Local {
	case "LifecycleConfiguration", "BucketLifecycleConfiguration":
	default:
		return xml.UnmarshalError(fmt.Sprintf("expected element type <LifecycleConfiguration>/<BucketLifecycleConfiguration> but have <%s>",
			start.Name.Local))
	}
	for {
		// Read tokens from the XML document in a stream.
		t, err := d.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch se := t.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "Rule":
				var r Rule
				if err = d.DecodeElement(&r, &se); err != nil {
					return err
				}
				lc.Rules = append(lc.Rules, r)
			default:
				return xml.UnmarshalError(fmt.Sprintf("expected element type <Rule> but have <%s>", se.Name.Local))
			}
		}
	}
	return nil
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

		if len(prefix) > 0 && len(rule.GetPrefix()) > 0 {
			if !recursive {
				// If not recursive, incoming prefix must be in rule prefix
				if !strings.HasPrefix(prefix, rule.GetPrefix()) {
					continue
				}
			}
			if recursive {
				// If recursive, we can skip this rule if it doesn't match the tested prefix.
				if !strings.HasPrefix(prefix, rule.GetPrefix()) && !strings.HasPrefix(rule.GetPrefix(), prefix) {
					continue
				}
			}
		}

		if rule.NoncurrentVersionExpiration.NoncurrentDays > 0 {
			return true
		}
		if rule.NoncurrentVersionTransition.NoncurrentDays > 0 {
			return true
		}
		if rule.Expiration.IsNull() && rule.Transition.IsNull() {
			continue
		}
		if !rule.Expiration.IsDateNull() && rule.Expiration.Date.Before(time.Now()) {
			return true
		}
		if !rule.Transition.IsDateNull() && rule.Transition.Date.Before(time.Now()) {
			return true
		}
		if !rule.Expiration.IsDaysNull() || !rule.Transition.IsDaysNull() {
			return true
		}
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
	// Make sure Rule ID is unique
	for i := range lc.Rules {
		if i == len(lc.Rules)-1 {
			break
		}
		otherRules := lc.Rules[i+1:]
		for _, otherRule := range otherRules {
			if lc.Rules[i].ID == otherRule.ID {
				return errLifecycleDuplicateID
			}
		}
	}
	return nil
}

// FilterActionableRules returns the rules actions that need to be executed
// after evaluating prefix/tag filtering
func (lc Lifecycle) FilterActionableRules(obj ObjectOpts) []Rule {
	if obj.Name == "" {
		return nil
	}
	var rules []Rule
	for _, rule := range lc.Rules {
		if rule.Status == Disabled {
			continue
		}
		if !strings.HasPrefix(obj.Name, rule.GetPrefix()) {
			continue
		}
		// Indicates whether MinIO will remove a delete marker with no
		// noncurrent versions. If set to true, the delete marker will
		// be expired; if set to false the policy takes no action. This
		// cannot be specified with Days or Date in a Lifecycle
		// Expiration Policy.
		if rule.Expiration.DeleteMarker.val {
			rules = append(rules, rule)
			continue
		}
		// The NoncurrentVersionExpiration action requests MinIO to expire
		// noncurrent versions of objects x days after the objects become
		// noncurrent.
		if !rule.NoncurrentVersionExpiration.IsDaysNull() {
			rules = append(rules, rule)
			continue
		}
		// The NoncurrentVersionTransition action requests MinIO to transition
		// noncurrent versions of objects x days after the objects become
		// noncurrent.
		if !rule.NoncurrentVersionTransition.IsDaysNull() {
			rules = append(rules, rule)
			continue
		}

		if rule.Filter.TestTags(strings.Split(obj.UserTags, "&")) {
			rules = append(rules, rule)
		}
		if !rule.Transition.IsNull() {
			rules = append(rules, rule)
		}
	}
	return rules
}

// ObjectOpts provides information to deduce the lifecycle actions
// which can be triggered on the resultant object.
type ObjectOpts struct {
	Name                   string
	UserTags               string
	ModTime                time.Time
	VersionID              string
	IsLatest               bool
	DeleteMarker           bool
	NumVersions            int
	SuccessorModTime       time.Time
	TransitionStatus       string
	RestoreOngoing         bool
	RestoreExpires         time.Time
	RemoteTiersImmediately []string // strictly for debug only
}

// doesMatchDebugTiers returns true if tier matches one of the debugTiers, false
// otherwise.
func doesMatchDebugTiers(tier string, debugTiers []string) bool {
	for _, t := range debugTiers {
		if strings.ToUpper(tier) == strings.ToUpper(t) {
			return true
		}
	}
	return false
}

// ExpiredObjectDeleteMarker returns true if an object version referred to by o
// is the only version remaining and is a delete marker. It returns false
// otherwise.
func (o ObjectOpts) ExpiredObjectDeleteMarker() bool {
	return o.DeleteMarker && o.NumVersions == 1
}

// ComputeAction returns the action to perform by evaluating all lifecycle rules
// against the object name and its modification time.
func (lc Lifecycle) ComputeAction(obj ObjectOpts) Action {
	var action = NoneAction
	if obj.ModTime.IsZero() {
		return action
	}
	for _, rule := range lc.FilterActionableRules(obj) {
		if obj.ExpiredObjectDeleteMarker() && rule.Expiration.DeleteMarker.val {
			// Indicates whether MinIO will remove a delete marker with no noncurrent versions.
			// Only latest marker is removed. If set to true, the delete marker will be expired;
			// if set to false the policy takes no action. This cannot be specified with Days or
			// Date in a Lifecycle Expiration Policy.
			return DeleteVersionAction
		}

		if !rule.NoncurrentVersionExpiration.IsDaysNull() {
			if obj.VersionID != "" && !obj.IsLatest && !obj.SuccessorModTime.IsZero() {
				// Non current versions should be deleted if their age exceeds non current days configuration
				// https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-actions
				if time.Now().After(ExpectedExpiryTime(obj.SuccessorModTime, int(rule.NoncurrentVersionExpiration.NoncurrentDays))) {
					return DeleteVersionAction
				}
			}

			if obj.VersionID != "" && obj.ExpiredObjectDeleteMarker() {
				// From https: //docs.aws.amazon.com/AmazonS3/latest/dev/lifecycle-configuration-examples.html :
				//   The NoncurrentVersionExpiration action in the same Lifecycle configuration removes noncurrent objects X days
				//   after they become noncurrent. Thus, in this example, all object versions are permanently removed X days after
				//   object creation. You will have expired object delete markers, but Amazon S3 detects and removes the expired
				//   object delete markers for you.
				if time.Now().After(ExpectedExpiryTime(obj.ModTime, int(rule.NoncurrentVersionExpiration.NoncurrentDays))) {
					return DeleteVersionAction
				}
			}
		}

		if !rule.NoncurrentVersionTransition.IsDaysNull() {
			if obj.VersionID != "" && !obj.IsLatest && !obj.SuccessorModTime.IsZero() && !obj.DeleteMarker && obj.TransitionStatus != TransitionComplete {
				// Non current versions should be transitioned if their age exceeds non current days configuration
				// https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-actions
				if time.Now().After(ExpectedExpiryTime(obj.SuccessorModTime, int(rule.NoncurrentVersionTransition.NoncurrentDays))) {
					return TransitionVersionAction
				}

				// this if condition is strictly for debug purposes to force immediate
				// transition to remote tier if _MINIO_DEBUG_REMOTE_TIERS_IMMEDIATELY is set
				if doesMatchDebugTiers(rule.NoncurrentVersionTransition.StorageClass, obj.RemoteTiersImmediately) {
					return TransitionVersionAction
				}
			}
		}

		// Remove the object or simply add a delete marker (once) in a versioned bucket
		if obj.VersionID == "" || obj.IsLatest && !obj.DeleteMarker {
			switch {
			case !rule.Expiration.IsDateNull():
				if time.Now().UTC().After(rule.Expiration.Date.Time) {
					return DeleteAction
				}
			case !rule.Expiration.IsDaysNull():
				if time.Now().UTC().After(ExpectedExpiryTime(obj.ModTime, int(rule.Expiration.Days))) {
					return DeleteAction
				}
			}

			if obj.TransitionStatus != TransitionComplete {
				switch {
				case !rule.Transition.IsDateNull():
					if time.Now().UTC().After(rule.Transition.Date.Time) {
						action = TransitionAction
					}
				case !rule.Transition.IsDaysNull():
					if time.Now().UTC().After(ExpectedExpiryTime(obj.ModTime, int(rule.Transition.Days))) {
						action = TransitionAction
					}

				}
				// this if condition is strictly for debug purposes to force immediate
				// transition to remote tier if _MINIO_DEBUG_REMOTE_TIERS_IMMEDIATELY is set
				if !rule.Transition.IsNull() && doesMatchDebugTiers(rule.Transition.StorageClass, obj.RemoteTiersImmediately) {
					action = TransitionAction
				}

				if !obj.RestoreExpires.IsZero() && time.Now().After(obj.RestoreExpires) {
					if obj.VersionID != "" {
						action = DeleteRestoredVersionAction
					} else {
						action = DeleteRestoredAction
					}
				}
			}
			if !obj.RestoreExpires.IsZero() && time.Now().After(obj.RestoreExpires) {
				if obj.VersionID != "" {
					action = DeleteRestoredVersionAction
				} else {
					action = DeleteRestoredAction
				}
			}

		}
	}
	return action
}

// ExpectedExpiryTime calculates the expiry, transition or restore date/time based on a object modtime.
// The expected transition or restore time is always a midnight time following the the object
// modification time plus the number of transition/restore days.
//   e.g. If the object modtime is `Thu May 21 13:42:50 GMT 2020` and the object should
//       transition in 1 day, then the expected transition time is `Fri, 23 May 2020 00:00:00 GMT`
func ExpectedExpiryTime(modTime time.Time, days int) time.Time {
	t := modTime.UTC().Add(time.Duration(days+1) * 24 * time.Hour)
	return t.Truncate(24 * time.Hour)
}

// PredictExpiryTime returns the expiry date/time of a given object
// after evaluating the current lifecycle document.
func (lc Lifecycle) PredictExpiryTime(obj ObjectOpts) (string, time.Time) {
	if obj.DeleteMarker {
		// We don't need to send any x-amz-expiration for delete marker.
		return "", time.Time{}
	}

	var finalExpiryDate time.Time
	var finalExpiryRuleID string

	// Iterate over all actionable rules and find the earliest
	// expiration date and its associated rule ID.
	for _, rule := range lc.FilterActionableRules(obj) {
		if !rule.NoncurrentVersionExpiration.IsDaysNull() && !obj.IsLatest && obj.VersionID != "" {
			return rule.ID, ExpectedExpiryTime(obj.SuccessorModTime, int(rule.NoncurrentVersionExpiration.NoncurrentDays))
		}

		if !rule.Expiration.IsDateNull() {
			if finalExpiryDate.IsZero() || finalExpiryDate.After(rule.Expiration.Date.Time) {
				finalExpiryRuleID = rule.ID
				finalExpiryDate = rule.Expiration.Date.Time
			}
		}
		if !rule.Expiration.IsDaysNull() {
			expectedExpiry := ExpectedExpiryTime(obj.ModTime, int(rule.Expiration.Days))
			if finalExpiryDate.IsZero() || finalExpiryDate.After(expectedExpiry) {
				finalExpiryRuleID = rule.ID
				finalExpiryDate = expectedExpiry
			}
		}
	}
	return finalExpiryRuleID, finalExpiryDate
}

// PredictTransitionTime returns the transition date/time of a given object
// after evaluating the current lifecycle document.
func (lc Lifecycle) PredictTransitionTime(obj ObjectOpts) (string, time.Time) {
	if obj.DeleteMarker {
		// We don't need to send any x-minio-transition for delete marker.
		return "", time.Time{}
	}

	if obj.TransitionStatus == TransitionComplete {
		return "", time.Time{}
	}

	var finalTransitionDate time.Time
	var finalTransitionRuleID string

	// Iterate over all actionable rules and find the earliest
	// transition date and its associated rule ID.
	for _, rule := range lc.FilterActionableRules(obj) {
		switch {
		case !rule.Transition.IsDateNull():
			if finalTransitionDate.IsZero() || finalTransitionDate.After(rule.Transition.Date.Time) {
				finalTransitionRuleID = rule.ID
				finalTransitionDate = rule.Transition.Date.Time
			}
		case !rule.Transition.IsDaysNull():
			expectedTransition := ExpectedExpiryTime(obj.ModTime, int(rule.Expiration.Days))
			if finalTransitionDate.IsZero() || finalTransitionDate.After(expectedTransition) {
				finalTransitionRuleID = rule.ID
				finalTransitionDate = expectedTransition
			}
		}
	}
	return finalTransitionRuleID, finalTransitionDate
}
