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
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/message/log"
	"github.com/minio/minio/cmd/logger/target/console"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/pubsub"
)

// number of log messages to buffer
const defaultLogBufferCount = 10000

//HTTPConsoleLoggerSys holds global console logger state
type HTTPConsoleLoggerSys struct {
	sync.RWMutex
	pubsub   *pubsub.PubSub
	console  *console.Target
	nodeName string
	logBuf   *ring.Ring
}

func mustGetNodeName(endpointZones EndpointZones) (nodeName string) {
	host, err := xnet.ParseHost(GetLocalPeer(endpointZones))
	if err != nil {
		logger.FatalIf(err, "Unable to start console logging subsystem")
	}
	if globalIsDistErasure {
		nodeName = host.Name
	}
	return nodeName
}

// NewConsoleLogger - creates new HTTPConsoleLoggerSys with all nodes subscribed to
// the console logging pub sub system
func NewConsoleLogger(ctx context.Context) *HTTPConsoleLoggerSys {
	ps := pubsub.New()
	return &HTTPConsoleLoggerSys{
		pubsub:  ps,
		console: console.New(),
		logBuf:  ring.New(defaultLogBufferCount),
	}
}

// SetNodeName - sets the node name if any after distributed setup has initialized
func (sys *HTTPConsoleLoggerSys) SetNodeName(endpointZones EndpointZones) {
	sys.nodeName = mustGetNodeName(endpointZones)
}

// HasLogListeners returns true if console log listeners are registered
// for this node or peers
func (sys *HTTPConsoleLoggerSys) HasLogListeners() bool {
	return sys != nil && sys.pubsub.HasSubscribers()
}

// Subscribe starts console logging for this node.
func (sys *HTTPConsoleLoggerSys) Subscribe(subCh chan interface{}, doneCh <-chan struct{}, node string, last int, logKind string, filter func(entry interface{}) bool) {
	// Enable console logging for remote client.
	if !sys.HasLogListeners() {
		logger.AddTarget(sys)
	}

	cnt := 0
	// by default send all console logs in the ring buffer unless node or limit query parameters
	// are set.
	var lastN []log.Info
	if last > defaultLogBufferCount || last <= 0 {
		last = defaultLogBufferCount
	}

	lastN = make([]log.Info, last)
	sys.RLock()
	sys.logBuf.Do(func(p interface{}) {
		if p != nil {
			lg, ok := p.(log.Info)
			if ok && lg.SendLog(node, logKind) {
				lastN[cnt%last] = lg
				cnt++
			}
		}
	})
	sys.RUnlock()
	// send last n console log messages in order filtered by node
	if cnt > 0 {
		for i := 0; i < last; i++ {
			entry := lastN[(cnt+i)%last]
			if (entry == log.Info{}) {
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

// Send log message 'e' to console and publish to console
// log pubsub system
func (sys *HTTPConsoleLoggerSys) Send(e interface{}, logKind string) error {
	var lg log.Info
	switch e := e.(type) {
	case log.Entry:
		lg = log.Info{Entry: e, NodeName: sys.nodeName}
	case string:
		lg = log.Info{ConsoleMsg: e, NodeName: sys.nodeName}
	}

	sys.pubsub.Publish(lg)
	sys.Lock()
	// add log to ring buffer
	sys.logBuf.Value = lg
	sys.logBuf = sys.logBuf.Next()
	sys.Unlock()

	return sys.console.Send(e, string(logger.All))
}
