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
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	ps := New[Maskable, Mask](2)
	ch1 := make(chan Maskable, 1)
	ch2 := make(chan Maskable, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(MaskAll, ch1, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(MaskAll, ch2, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ps.Lock()
	defer ps.Unlock()

	if len(ps.subs) != 2 || ps.NumSubscribers(MaskAll) != 2 || ps.Subscribers() != 2 {
		t.Fatalf("expected 2 subscribers")
	}
}

func TestNumSubscribersMask(t *testing.T) {
	ps := New[Maskable, Mask](2)
	ch1 := make(chan Maskable, 1)
	ch2 := make(chan Maskable, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(Mask(1), ch1, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(Mask(2), ch2, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ps.Lock()
	defer ps.Unlock()

	if len(ps.subs) != 2 {
		t.Fatalf("expected 2 subscribers")
	}
	if want, got := int32(2), ps.NumSubscribers(Mask(1)); got != want {
		t.Fatalf("want %d subscribers, got %d", want, got)
	}
	if want, got := int32(2), ps.NumSubscribers(Mask(2)); got != want {
		t.Fatalf("want %d subscribers, got %d", want, got)
	}
	if want, got := int32(2), ps.NumSubscribers(Mask(1|2)); got != want {
		t.Fatalf("want %d subscribers, got %d", want, got)
	}
	if want, got := int32(2), ps.NumSubscribers(MaskAll); got != want {
		t.Fatalf("want %d subscribers, got %d", want, got)
	}
	if want, got := int32(0), ps.NumSubscribers(Mask(4)); got != want {
		t.Fatalf("want %d subscribers, got %d", want, got)
	}
}

func TestSubscribeExceedingLimit(t *testing.T) {
	ps := New[Maskable, Maskable](2)
	ch1 := make(chan Maskable, 1)
	ch2 := make(chan Maskable, 1)
	ch3 := make(chan Maskable, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(MaskAll, ch1, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(MaskAll, ch2, doneCh, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(MaskAll, ch3, doneCh, nil); err == nil {
		t.Fatalf("unexpected nil err")
	}
}

func TestUnsubscribe(t *testing.T) {
	ps := New[Maskable, Maskable](2)
	ch1 := make(chan Maskable, 1)
	ch2 := make(chan Maskable, 1)
	doneCh1 := make(chan struct{})
	doneCh2 := make(chan struct{})
	if err := ps.Subscribe(MaskAll, ch1, doneCh1, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(MaskAll, ch2, doneCh2, nil); err != nil {
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

type maskString string

func (m maskString) Mask() uint64 {
	return 1
}

func TestPubSub(t *testing.T) {
	ps := New[Maskable, Maskable](1)
	ch1 := make(chan Maskable, 1)
	doneCh1 := make(chan struct{})
	defer close(doneCh1)
	if err := ps.Subscribe(MaskAll, ch1, doneCh1, func(entry Maskable) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := maskString("hello")
	ps.Publish(val)
	msg := <-ch1
	if msg != val {
		t.Fatalf("expected %s , found %s", val, msg)
	}
}

func TestMultiPubSub(t *testing.T) {
	ps := New[Maskable, Maskable](2)
	ch1 := make(chan Maskable, 1)
	ch2 := make(chan Maskable, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	if err := ps.Subscribe(MaskAll, ch1, doneCh, func(entry Maskable) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ps.Subscribe(MaskAll, ch2, doneCh, func(entry Maskable) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := maskString("hello")
	ps.Publish(val)

	msg1 := <-ch1
	msg2 := <-ch2
	if msg1 != val && msg2 != val {
		t.Fatalf("expected both subscribers to have%s , found %s and  %s", val, msg1, msg2)
	}
}

func TestMultiPubSubMask(t *testing.T) {
	ps := New[Maskable, Maskable](3)
	ch1 := make(chan Maskable, 1)
	ch2 := make(chan Maskable, 1)
	ch3 := make(chan Maskable, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	// Mask matches maskString, should get result
	if err := ps.Subscribe(Mask(1), ch1, doneCh, func(entry Maskable) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Mask matches maskString, should get result
	if err := ps.Subscribe(Mask(1|2), ch2, doneCh, func(entry Maskable) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Does NOT overlap maskString
	if err := ps.Subscribe(Mask(2), ch3, doneCh, func(entry Maskable) bool { return true }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	val := maskString("hello")
	ps.Publish(val)

	msg1 := <-ch1
	msg2 := <-ch2
	if msg1 != val && msg2 != val {
		t.Fatalf("expected both subscribers to have%s , found %s and  %s", val, msg1, msg2)
	}

	select {
	case msg := <-ch3:
		t.Fatalf("unexpected msg, f got %s", msg)
	default:
	}
}
