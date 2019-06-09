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
)

func TestSubscribe(t *testing.T) {
	ps := New()
	ps.Subscribe()
	ps.Subscribe()
	if len(ps.subs) != 2 {
		t.Errorf("expected 2 subscribers")
	}
}

func TestUnsubscribe(t *testing.T) {
	ps := New()
	c1 := ps.Subscribe()
	ps.Subscribe()
	ps.Unsubscribe(c1)
	if len(ps.subs) != 1 {
		t.Errorf("expected 1 subscriber")
	}
}

func TestPubSub(t *testing.T) {
	ps := New()
	c1 := ps.Subscribe()
	val := "hello"
	ps.Publish(val)
	msg := <-c1
	if msg != "hello" {
		t.Errorf(fmt.Sprintf("expected %s , found %s", val, msg))
	}
}

func TestMultiPubSub(t *testing.T) {
	ps := New()
	c1 := ps.Subscribe()
	c2 := ps.Subscribe()
	val := "hello"
	ps.Publish(val)

	msg1 := <-c1
	msg2 := <-c2
	if msg1 != "hello" && msg2 != "hello" {
		t.Errorf(fmt.Sprintf("expected both subscribers to have%s , found %s and  %s", val, msg1, msg2))
	}
}
