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

package pubsub

import (
	"fmt"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	ps := New(2)
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(ch1, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(ch2, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ps.Lock()
	defer ps.Unlock()
	if len(ps.subs) != 2 {
		t.Fatalf("expected 2 subscribers")
	}
}

func TestSubscribeExceedingLimit(t *testing.T) {
	ps := New(2)
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	ch3 := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(ch1, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(ch2, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(ch3, doneCh, nil); err == nil {
		t.Fatalf("unexpected nil err")
	}
}

func TestUnsubscribe(t *testing.T) {
	ps := New(2)
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	doneCh1 := make(chan struct{})
	doneCh2 := make(chan struct{})
	if err := ps.Subscribe(ch1, doneCh1, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(ch2, doneCh2, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(doneCh1)
	// Allow for the above statement to take effect.
	time.Sleep(100 * time.Millisecond)
	ps.Lock()
	if len(ps.subs) != 1 {
		t.Fatal("expected 1 subscriber")
	}
	ps.Unlock()
	close(doneCh2)
}

func TestPubSub(t *testing.T) {
	ps := New(1)
	ch1 := make(chan interface{}, 1)
	doneCh1 := make(chan struct{})
	defer close(doneCh1)
	if err := ps.Subscribe(ch1, doneCh1, func(entry interface{}) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := "hello"
	ps.Publish(val)
	msg := <-ch1
	if msg != "hello" {
		t.Fatalf(fmt.Sprintf("expected %s , found %s", val, msg))
	}
}

func TestMultiPubSub(t *testing.T) {
	ps := New(2)
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(ch1, doneCh, func(entry interface{}) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(ch2, doneCh, func(entry interface{}) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := "hello"
	ps.Publish(val)

	msg1 := <-ch1
	msg2 := <-ch2
	if msg1 != "hello" && msg2 != "hello" {
		t.Fatalf(fmt.Sprintf("expected both subscribers to have%s , found %s and  %s", val, msg1, msg2))
	}
}
