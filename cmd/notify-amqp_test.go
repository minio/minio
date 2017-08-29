/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package cmd

import (
	"errors"
	"net"
	"testing"

	"github.com/streadway/amqp"
)

// Tests for is closed network error.
func TestIsClosedNetworkErr(t *testing.T) {
	testCases := []struct {
		err     error
		success bool
	}{
		{
			err:     amqp.ErrClosed,
			success: true,
		},
		{
			err:     &net.OpError{Err: errors.New("use of closed network connection")},
			success: true,
		},
		{
			err:     nil,
			success: false,
		},
		{
			err:     errors.New("testing error"),
			success: false,
		},
	}

	for i, testCase := range testCases {
		ok := isAMQPClosedNetworkErr(testCase.err)
		if ok != testCase.success {
			t.Errorf("Test %d: Expected %t, got %t", i+1, testCase.success, ok)
		}
	}
}
