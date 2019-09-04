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

package cmd

import (
	ring "container/ring"
	"context"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/message/log"
	"github.com/minio/minio/cmd/logger/target/console"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/pubsub"
)

// number of log messages to buffer
const defaultLogBufferCount = 10000

//HTTPConsoleLoggerSys holds global console logger state
type HTTPConsoleLoggerSys struct {
	pubsub   *pubsub.PubSub
	console  *console.Target
	nodeName string
	logBuf   *ring.Ring
}

// NewConsoleLogger - creates new HTTPConsoleLoggerSys with all nodes subscribed to
// the console logging pub sub system
func NewConsoleLogger(ctx context.Context, endpoints EndpointList) *HTTPConsoleLoggerSys {
	host, err := xnet.ParseHost(GetLocalPeer(globalEndpoints))
	if err != nil {
		logger.FatalIf(err, "Unable to start console logging subsystem")
	}
	var nodeName string
	if globalIsDistXL {
		nodeName = host.Name
	}
	ps := pubsub.New()
	return &HTTPConsoleLoggerSys{
		ps, nil, nodeName, ring.New(defaultLogBufferCount),
	}
}

// HasLogListeners returns true if console log listeners are registered
// for this node or peers
func (sys *HTTPConsoleLoggerSys) HasLogListeners() bool {
	return sys != nil && sys.pubsub.HasSubscribers()
}

// Subscribe starts console logging for this node.
func (sys *HTTPConsoleLoggerSys) Subscribe(subCh chan interface{}, doneCh chan struct{}, node string, last int, filter func(entry interface{}) bool) {
	// Enable console logging for remote client even if local console logging is disabled in the config.
	if !globalServerConfig.Logger.Console.Enabled && !sys.pubsub.HasSubscribers() {
		logger.AddTarget(globalConsoleSys.Console())
	}

	cnt := 0
	// by default send all console logs in the ring buffer unless node or limit query parameters
	// are set.
	var lastN []madmin.LogInfo
	if last > defaultLogBufferCount || last <= 0 {
		last = defaultLogBufferCount
	}

	lastN = make([]madmin.LogInfo, last)
	r := sys.logBuf
	r.Do(func(p interface{}) {
		if p != nil && (p.(madmin.LogInfo)).SendLog(node) {
			lastN[cnt%last] = p.(madmin.LogInfo)
			cnt++
		}
	})
	// send last n console log messages in order filtered by node
	if cnt > 0 {
		for i := 0; i < last; i++ {
			entry := lastN[(cnt+i)%last]
			if (entry == madmin.LogInfo{}) {
				continue
			}
			select {
			case subCh <- entry:
			case <-doneCh:
				return
			}
		}
	}
	sys.pubsub.Subscribe(subCh, doneCh, filter)
}

// Console returns a  console target
func (sys *HTTPConsoleLoggerSys) Console() *HTTPConsoleLoggerSys {
	if sys.console == nil {
		sys.console = console.New()
	}
	return sys
}

// Send log message 'e' to console and publish to console
// log pubsub system
func (sys *HTTPConsoleLoggerSys) Send(e interface{}) error {
	lg := madmin.LogInfo{}
	lg.Entry = e.(log.Entry)
	lg.NodeName = sys.nodeName
	sys.pubsub.Publish(lg)
	// add log to ring buffer
	sys.logBuf.Value = lg
	sys.logBuf = sys.logBuf.Next()

	if globalServerConfig.Logger.Console.Enabled {
		return sys.console.Send(e)
	}
	return nil
}
