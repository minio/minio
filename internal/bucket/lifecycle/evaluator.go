// Copyright (c) 2015-2025 MinIO, Inc.
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
	"fmt"
	"time"

	objlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
)

// Evaluator - evaluates lifecycle policy on objects for the given lifecycle
// configuration, lock retention configuration and replication configuration.
type Evaluator struct {
	policy        Lifecycle
	lockRetention *objlock.Retention
	replCfg       *replication.Config
}

// NewEvaluator - creates a new evaluator with the given lifecycle
func NewEvaluator(policy Lifecycle) *Evaluator {
	return &Evaluator{
		policy: policy,
	}
}

// WithLockRetention - sets the lock retention configuration for the evaluator
func (e *Evaluator) WithLockRetention(lr *objlock.Retention) *Evaluator {
	e.lockRetention = lr
	return e
}

// WithReplicationConfig - sets the replication configuration for the evaluator
func (e *Evaluator) WithReplicationConfig(rcfg *replication.Config) *Evaluator {
	e.replCfg = rcfg
	return e
}

// IsPendingReplication checks if the object is pending replication.
func (e *Evaluator) IsPendingReplication(obj ObjectOpts) bool {
	if e.replCfg == nil {
		return false
	}
	if e.replCfg.HasActiveRules(obj.Name, true) && !obj.VersionPurgeStatus.Empty() {
		return true
	}

	return false
}

// IsObjectLocked checks if it is appropriate to remove an
// object according to locking configuration when this is lifecycle/ bucket quota asking.
// (copied over from enforceRetentionForDeletion)
func (e *Evaluator) IsObjectLocked(obj ObjectOpts) bool {
	if e.lockRetention == nil || !e.lockRetention.LockEnabled {
		return false
	}

	if obj.DeleteMarker {
		return false
	}

	lhold := objlock.GetObjectLegalHoldMeta(obj.UserDefined)
	if lhold.Status.Valid() && lhold.Status == objlock.LegalHoldOn {
		return true
	}

	ret := objlock.GetObjectRetentionMeta(obj.UserDefined)
	if ret.Mode.Valid() && (ret.Mode == objlock.RetCompliance || ret.Mode == objlock.RetGovernance) {
		t, err := objlock.UTCNowNTP()
		if err != nil {
			// it is safe to assume that the object is locked when
			// we can't get the current time
			return true
		}
		if ret.RetainUntilDate.After(t) {
			return true
		}
	}
	return false
}

// eval will return a lifecycle event for each object in objs for a given time.
func (e *Evaluator) eval(objs []ObjectOpts, now time.Time) []Event {
	events := make([]Event, len(objs))
	var newerNoncurrentVersions int
loop:
	for i, obj := range objs {
		event := e.policy.eval(obj, now, newerNoncurrentVersions)
		switch event.Action {
		case DeleteAllVersionsAction, DelMarkerDeleteAllVersionsAction:
			// Skip if bucket has object locking enabled; To prevent the
			// possibility of violating an object retention on one of the
			// noncurrent versions of this object.
			if e.lockRetention != nil && e.lockRetention.LockEnabled {
				event = Event{}
			} else {
				// No need to evaluate remaining versions' lifecycle
				// events after DeleteAllVersionsAction*
				events[i] = event
				break loop
			}

		case DeleteVersionAction, DeleteRestoredVersionAction:
			// Defensive code, should never happen
			if obj.VersionID == "" {
				event.Action = NoneAction
			}
			if e.IsObjectLocked(obj) {
				event = Event{}
			}

			if e.IsPendingReplication(obj) {
				event = Event{}
			}
		}
		if !obj.IsLatest {
			switch event.Action {
			case DeleteVersionAction:
				// this noncurrent version will be expired, nothing to add
			default:
				// this noncurrent version will be spared
				newerNoncurrentVersions++
			}
		}
		events[i] = event
	}
	return events
}

// Eval will return a lifecycle event for each object in objs
func (e *Evaluator) Eval(objs []ObjectOpts) ([]Event, error) {
	if len(objs) == 0 {
		return nil, nil
	}
	if len(objs) != objs[0].NumVersions {
		return nil, fmt.Errorf("number of versions mismatch, expected %d, got %d", objs[0].NumVersions, len(objs))
	}
	return e.eval(objs, time.Now().UTC()), nil
}
