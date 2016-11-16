/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"fmt"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
)

type listenerConn struct {
	TargetAddr  string
	ListenerARN string
	BMSClient   BucketMetaState
}

type listenerLogger struct {
	log   *logrus.Logger
	lconn listenerConn
}

func newListenerLogger(listenerArn, targetAddr string) (*listenerLogger, error) {
	bmsClient := globalS3Peers.GetPeerClient(targetAddr)
	if bmsClient == nil {
		return nil, fmt.Errorf(
			"Peer %s was not initialized, unexpected error",
			targetAddr,
		)
	}
	lc := listenerConn{
		TargetAddr:  targetAddr,
		ListenerARN: listenerArn,
		BMSClient:   bmsClient,
	}

	lcLog := logrus.New()

	lcLog.Out = ioutil.Discard

	lcLog.Formatter = new(logrus.JSONFormatter)

	lcLog.Hooks.Add(lc)

	return &listenerLogger{lcLog, lc}, nil
}

// send event to target server via rpc client calls.
func (lc listenerConn) Fire(entry *logrus.Entry) error {
	notificationEvent, ok := entry.Data["Records"].([]NotificationEvent)
	if !ok {
		// If the record is not of the expected type, silently
		// discard.
		return nil
	}

	// Send Event RPC call and return error
	arg := EventArgs{Event: notificationEvent, Arn: lc.ListenerARN}
	return lc.BMSClient.SendEvent(&arg)
}

func (lc listenerConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}
