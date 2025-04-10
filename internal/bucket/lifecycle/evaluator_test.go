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
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewerNoncurrentVersions(t *testing.T) {
	prepLifecycleCfg := func(tagKeys []string, retainVersions []int) Lifecycle {
		var lc Lifecycle
		for i := range retainVersions {
			ruleID := fmt.Sprintf("rule-%d", i)
			tag := Tag{
				Key:   tagKeys[i],
				Value: "minio",
			}
			lc.Rules = append(lc.Rules, Rule{
				ID:     ruleID,
				Status: "Enabled",
				Filter: Filter{
					Tag: tag,
					set: true,
				},
				NoncurrentVersionExpiration: NoncurrentVersionExpiration{
					NewerNoncurrentVersions: retainVersions[i],
					set:                     true,
				},
			})
		}
		return lc
	}

	lc := prepLifecycleCfg([]string{"tag3", "tag4", "tag5"}, []int{3, 4, 5})
	evaluator := NewEvaluator(lc)
	tagKeys := []string{"tag3", "tag3", "tag3", "tag4", "tag4", "tag5", "tag5"}
	verIDs := []string{
		"0NdAikoUVNGEpCUuB9vl.XyoMftMXCSg", "19M6Z405yFZuYygnnU9jKzsOBamTZK_7", "0PmlJdFWi_9d6l_dAkWrrhP.bBgtFk6V", // spellchecker:disable-line
		".MmRalFNNJyOLymgCtQ3.qsdoYpy8qkB", "Bjb4OlMW9Agx.Nrggh15iU6frGu2CLde", "ngBmUd_cVl6ckONI9XsKGpJjzimohrzZ", // spellchecker:disable-line
		"T6m1heTHLUtnByW2IOWJ3zM4JP9xXt2O", // spellchecker:disable-line
	}
	wantEvents := []Event{
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: DeleteVersionAction},
	}
	var objs []ObjectOpts
	curModTime := time.Date(2025, time.February, 10, 23, 0, 0, 0, time.UTC)
	for i := range tagKeys {
		obj := ObjectOpts{
			Name:        "obj",
			VersionID:   verIDs[i],
			ModTime:     curModTime.Add(time.Duration(-i) * time.Second),
			UserTags:    fmt.Sprintf("%s=minio", tagKeys[i]),
			NumVersions: len(verIDs),
		}
		if i == 0 {
			obj.IsLatest = true
		} else {
			obj.SuccessorModTime = curModTime.Add(time.Duration(-i+1) * time.Second)
		}
		objs = append(objs, obj)
	}
	now := time.Date(2025, time.February, 10, 23, 0, 0, 0, time.UTC)
	gotEvents := evaluator.eval(objs, now)
	for i := range wantEvents {
		if gotEvents[i].Action != wantEvents[i].Action {
			t.Fatalf("got %v, want %v", gotEvents[i], wantEvents[i])
		}
	}

	lc = prepLifecycleCfg([]string{"tag3", "tag4", "tag5"}, []int{1, 2, 3})
	objs = objs[:len(objs)-1]
	wantEvents = []Event{
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: DeleteVersionAction},
		{Action: NoneAction},
		{Action: DeleteVersionAction},
		{Action: NoneAction},
	}
	evaluator = NewEvaluator(lc)
	gotEvents = evaluator.eval(objs, now)
	for i := range wantEvents {
		if gotEvents[i].Action != wantEvents[i].Action {
			t.Fatalf("test-%d: got %v, want %v", i+1, gotEvents[i], wantEvents[i])
		}
	}

	lc = Lifecycle{
		Rules: []Rule{
			{
				ID:     "AllVersionsExpiration",
				Status: "Enabled",
				Filter: Filter{},
				Expiration: Expiration{
					Days: 1,
					DeleteAll: Boolean{
						val: true,
						set: true,
					},
					set: true,
				},
			},
		},
	}

	now = time.Date(2025, time.February, 12, 23, 0, 0, 0, time.UTC)
	evaluator = NewEvaluator(lc)
	gotEvents = evaluator.eval(objs, now)
	wantEvents = []Event{
		{Action: DeleteAllVersionsAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
		{Action: NoneAction},
	}
	for i := range wantEvents {
		if gotEvents[i].Action != wantEvents[i].Action {
			t.Fatalf("test-%d: got %v, want %v", i+1, gotEvents[i], wantEvents[i])
		}
	}

	// Test with zero versions
	events, err := evaluator.Eval(nil)
	if len(events) != 0 || err != nil {
		t.Fatal("expected no events nor error")
	}
}

func TestEmptyEvaluator(t *testing.T) {
	var objs []ObjectOpts
	curModTime := time.Date(2025, time.February, 10, 23, 0, 0, 0, time.UTC)
	for i := range 5 {
		obj := ObjectOpts{
			Name:        "obj",
			VersionID:   uuid.New().String(),
			ModTime:     curModTime.Add(time.Duration(-i) * time.Second),
			NumVersions: 5,
		}
		if i == 0 {
			obj.IsLatest = true
		} else {
			obj.SuccessorModTime = curModTime.Add(time.Duration(-i+1) * time.Second)
		}
		objs = append(objs, obj)
	}

	evaluator := NewEvaluator(Lifecycle{})
	events, err := evaluator.Eval(objs)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Action != NoneAction {
			t.Fatalf("got %v, want %v", event.Action, NoneAction)
		}
	}
}
