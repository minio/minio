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
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	xhttp "github.com/minio/minio/internal/http"
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
	// TransitionVersionAction transitions a particular object version after evaluating lifecycle transition rules
	TransitionVersionAction
	// DeleteRestoredAction means the temporarily restored object needs to be removed after evaluating lifecycle rules
	DeleteRestoredAction
	// DeleteRestoredVersionAction deletes a particular version that was temporarily restored
	DeleteRestoredVersionAction

	// ActionCount must be the last action and shouldn't be used as a regular action.
	ActionCount
)

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// HasTransition returns 'true' if lifecycle document has Transition enabled.
func (lc Lifecycle) HasTransition() bool {
	for _, rule := range lc.Rules {
		if rule.Transition.IsEnabled() {
			return true
		}
	}
	return false
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
		if rule.NoncurrentVersionExpiration.NewerNoncurrentVersions > 0 {
			return true
		}
		if !rule.NoncurrentVersionTransition.IsNull() {
			return true
		}
		if rule.Expiration.IsNull() && rule.Transition.IsNull() {
			continue
		}
		if !rule.Expiration.IsDateNull() && rule.Expiration.Date.Before(time.Now().UTC()) {
			return true
		}
		if !rule.Expiration.IsDaysNull() {
			return true
		}
		if !rule.Transition.IsDateNull() && rule.Transition.Date.Before(time.Now().UTC()) {
			return true
		}
		if !rule.Transition.IsNull() { // this allows for Transition.Days to be zero.
			return true
		}

	}
	return false
}

// ParseLifecycleConfigWithID - parses for a Lifecycle config and assigns
// unique id to rules with empty ID.
func ParseLifecycleConfigWithID(r io.Reader) (*Lifecycle, error) {
	var lc Lifecycle
	if err := xml.NewDecoder(r).Decode(&lc); err != nil {
		return nil, err
	}
	// assign a unique id for rules with empty ID
	for i := range lc.Rules {
		if lc.Rules[i].ID == "" {
			lc.Rules[i].ID = uuid.New().String()
		}
	}
	return &lc, nil
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

// FilterRules returns the rules filtered by the status, prefix and tags
func (lc Lifecycle) FilterRules(obj ObjectOpts) []Rule {
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
		if !rule.Filter.TestTags(obj.UserTags) {
			continue
		}
		rules = append(rules, rule)
	}
	return rules
}

// ObjectOpts provides information to deduce the lifecycle actions
// which can be triggered on the resultant object.
type ObjectOpts struct {
	Name             string
	UserTags         string
	ModTime          time.Time
	VersionID        string
	IsLatest         bool
	DeleteMarker     bool
	NumVersions      int
	SuccessorModTime time.Time
	TransitionStatus string
	RestoreOngoing   bool
	RestoreExpires   time.Time
}

// ExpiredObjectDeleteMarker returns true if an object version referred to by o
// is the only version remaining and is a delete marker. It returns false
// otherwise.
func (o ObjectOpts) ExpiredObjectDeleteMarker() bool {
	return o.DeleteMarker && o.NumVersions == 1
}

// Event contains a lifecycle action with associated info
type Event struct {
	Action       Action
	RuleID       string
	Due          time.Time
	StorageClass string
}

type lifecycleEvents []Event

func (es lifecycleEvents) Len() int {
	return len(es)
}

func (es lifecycleEvents) Swap(i, j int) {
	es[i], es[j] = es[j], es[i]
}

func (es lifecycleEvents) Less(i, j int) bool {
	if es[i].Due.Equal(es[j].Due) {
		// Prefer Expiration over Transition for both current and noncurrent
		// versions
		switch es[i].Action {
		case DeleteAction, DeleteVersionAction:
			return true
		}
		switch es[j].Action {
		case DeleteAction, DeleteVersionAction:
			return false
		}
		return true
	}

	// Prefer earlier occurring event
	return es[i].Due.Before(es[j].Due)
}

// Eval returns the lifecycle event applicable at now. If now is the zero value of time.Time, it returns the upcoming lifecycle event.
func (lc Lifecycle) Eval(obj ObjectOpts, now time.Time) Event {
	var events []Event
	if obj.ModTime.IsZero() {
		return Event{}
	}

	// Handle expiry of restored object; NB Restored Objects have expiry set on
	// them as part of RestoreObject API. They aren't governed by lifecycle
	// rules.
	if !obj.RestoreExpires.IsZero() && now.After(obj.RestoreExpires) {
		action := DeleteRestoredAction
		if !obj.IsLatest {
			action = DeleteRestoredVersionAction
		}

		events = append(events, Event{
			Action: action,
			Due:    now,
		})
	}

	for _, rule := range lc.FilterRules(obj) {
		if obj.ExpiredObjectDeleteMarker() {
			if rule.Expiration.DeleteMarker.val {
				// Indicates whether MinIO will remove a delete marker with no noncurrent versions.
				// Only latest marker is removed. If set to true, the delete marker will be expired;
				// if set to false the policy takes no action. This cannot be specified with Days or
				// Date in a Lifecycle Expiration Policy.
				events = append(events, Event{
					Action: DeleteVersionAction,
					RuleID: rule.ID,
					Due:    now,
				})
				// No other conflicting actions apply to an expired object delete marker
				break
			}

			if !rule.Expiration.IsDaysNull() {
				// Specifying the Days tag will automatically perform ExpiredObjectDeleteMarker cleanup
				// once delete markers are old enough to satisfy the age criteria.
				// https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
				if expectedExpiry := ExpectedExpiryTime(obj.ModTime, int(rule.Expiration.Days)); now.After(expectedExpiry) {
					events = append(events, Event{
						Action: DeleteVersionAction,
						RuleID: rule.ID,
						Due:    expectedExpiry,
					})
					// No other conflicting actions apply to an expired object delete marker
					break
				}
			}
		}

		// Skip rules with newer noncurrent versions specified. These rules are
		// not handled at an individual version level. ComputeAction applies
		// only to a specific version.
		if !obj.IsLatest && rule.NoncurrentVersionExpiration.NewerNoncurrentVersions > 0 {
			continue
		}

		if !obj.IsLatest && !rule.NoncurrentVersionExpiration.IsDaysNull() {
			// Non current versions should be deleted if their age exceeds non current days configuration
			// https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-actions
			if expectedExpiry := ExpectedExpiryTime(obj.SuccessorModTime, int(rule.NoncurrentVersionExpiration.NoncurrentDays)); now.After(expectedExpiry) {
				events = append(events, Event{
					Action: DeleteVersionAction,
					RuleID: rule.ID,
					Due:    expectedExpiry,
				})
			}
		}

		if !obj.IsLatest && !rule.NoncurrentVersionTransition.IsNull() {
			if !obj.DeleteMarker && obj.TransitionStatus != TransitionComplete {
				// Non current versions should be transitioned if their age exceeds non current days configuration
				// https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-actions
				if due, ok := rule.NoncurrentVersionTransition.NextDue(obj); ok && now.After(due) {
					events = append(events, Event{
						Action:       TransitionVersionAction,
						RuleID:       rule.ID,
						Due:          due,
						StorageClass: rule.NoncurrentVersionTransition.StorageClass,
					})
				}
			}
		}

		// Remove the object or simply add a delete marker (once) in a versioned bucket
		if obj.IsLatest && !obj.DeleteMarker {
			switch {
			case !rule.Expiration.IsDateNull():
				if time.Now().UTC().After(rule.Expiration.Date.Time) {
					events = append(events, Event{
						Action: DeleteAction,
						RuleID: rule.ID,
						Due:    rule.Expiration.Date.Time,
					})
				}
			case !rule.Expiration.IsDaysNull():
				if expectedExpiry := ExpectedExpiryTime(obj.ModTime, int(rule.Expiration.Days)); now.After(expectedExpiry) {
					events = append(events, Event{
						Action: DeleteAction,
						RuleID: rule.ID,
						Due:    expectedExpiry,
					})
				}
			}

			if obj.TransitionStatus != TransitionComplete {
				if due, ok := rule.Transition.NextDue(obj); ok && now.After(due) {
					events = append(events, Event{
						Action:       TransitionAction,
						RuleID:       rule.ID,
						Due:          due,
						StorageClass: rule.Transition.StorageClass,
					})
				}
			}
		}
	}

	if len(events) > 0 {
		sort.Sort(lifecycleEvents(events))
		return events[0]
	}

	return Event{
		Action: NoneAction,
	}
}

// ComputeAction returns the action to perform by evaluating all lifecycle rules
// against the object name and its modification time.
func (lc Lifecycle) ComputeAction(obj ObjectOpts) Action {
	return lc.Eval(obj, time.Now().UTC()).Action
}

// ExpectedExpiryTime calculates the expiry, transition or restore date/time based on a object modtime.
// The expected transition or restore time is always a midnight time following the object
// modification time plus the number of transition/restore days.
//
//	e.g. If the object modtime is `Thu May 21 13:42:50 GMT 2020` and the object should
//	    transition in 1 day, then the expected transition time is `Fri, 23 May 2020 00:00:00 GMT`
func ExpectedExpiryTime(modTime time.Time, days int) time.Time {
	if days == 0 {
		return modTime
	}
	t := modTime.UTC().Add(time.Duration(days+1) * 24 * time.Hour)
	return t.Truncate(24 * time.Hour)
}

// SetPredictionHeaders sets time to expiry and transition headers on w for a
// given obj.
func (lc Lifecycle) SetPredictionHeaders(w http.ResponseWriter, obj ObjectOpts) {
	event := lc.Eval(obj, time.Time{})
	switch event.Action {
	case DeleteAction, DeleteVersionAction:
		w.Header()[xhttp.AmzExpiration] = []string{
			fmt.Sprintf(`expiry-date="%s", rule-id="%s"`, event.Due.Format(http.TimeFormat), event.RuleID),
		}
	case TransitionAction, TransitionVersionAction:
		w.Header()[xhttp.MinIOTransition] = []string{
			fmt.Sprintf(`transition-date="%s", rule-id="%s"`, event.Due.Format(http.TimeFormat), event.RuleID),
		}
	}
}

// NoncurrentVersionsExpirationLimit returns the number of noncurrent versions
// to be retained from the first applicable rule per S3 behavior.
func (lc Lifecycle) NoncurrentVersionsExpirationLimit(obj ObjectOpts) (string, int, int) {
	var lim int
	var days int
	var ruleID string
	for _, rule := range lc.FilterRules(obj) {
		if rule.NoncurrentVersionExpiration.NewerNoncurrentVersions == 0 {
			continue
		}
		return rule.ID, int(rule.NoncurrentVersionExpiration.NoncurrentDays), rule.NoncurrentVersionExpiration.NewerNoncurrentVersions
	}
	return ruleID, days, lim
}
