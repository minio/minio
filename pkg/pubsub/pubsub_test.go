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

package pubsub

import (
	"fmt"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	ps := New()
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	ps.Subscribe(ch1, doneCh, nil)
	ps.Subscribe(ch2, doneCh, nil)
	ps.Lock()
	defer ps.Unlock()
	if len(ps.subs) != 2 {
		t.Errorf("expected 2 subscribers")
	}
}

func TestUnsubscribe(t *testing.T) {
	ps := New()
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	doneCh1 := make(chan struct{})
	doneCh2 := make(chan struct{})
	ps.Subscribe(ch1, doneCh1, nil)
	ps.Subscribe(ch2, doneCh2, nil)

	close(doneCh1)
	// Allow for the above statement to take effect.
	time.Sleep(100 * time.Millisecond)
	ps.Lock()
	if len(ps.subs) != 1 {
		t.Errorf("expected 1 subscriber")
	}
	ps.Unlock()
	close(doneCh2)
}

func TestPubSub(t *testing.T) {
	ps := New()
	ch1 := make(chan interface{}, 1)
	doneCh1 := make(chan struct{})
	defer close(doneCh1)
	ps.Subscribe(ch1, doneCh1, func(entry interface{}) bool { return true })
	val := "hello"
	ps.Publish(val)
	msg := <-ch1
	if msg != "hello" {
		t.Errorf(fmt.Sprintf("expected %s , found %s", val, msg))
	}
}

func TestMultiPubSub(t *testing.T) {
	ps := New()
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)
	ps.Subscribe(ch1, doneCh, func(entry interface{}) bool { return true })
	ps.Subscribe(ch2, doneCh, func(entry interface{}) bool { return true })
	val := "hello"
	ps.Publish(val)

	msg1 := <-ch1
	msg2 := <-ch2
	if msg1 != "hello" && msg2 != "hello" {
		t.Errorf(fmt.Sprintf("expected both subscribers to have%s , found %s and  %s", val, msg1, msg2))
	}
}
