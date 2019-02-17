/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func keepalive(c *client) {
	defer c.workers.Done()
	DEBUG.Println(PNG, "keepalive starting")
	var checkInterval int64
	var pingSent time.Time

	if c.options.KeepAlive > 10 {
		checkInterval = 5
	} else {
		checkInterval = c.options.KeepAlive / 2
	}

	intervalTicker := time.NewTicker(time.Duration(checkInterval * int64(time.Second)))
	defer intervalTicker.Stop()

	for {
		select {
		case <-c.stop:
			DEBUG.Println(PNG, "keepalive stopped")
			return
		case <-intervalTicker.C:
			DEBUG.Println(PNG, "ping check", time.Now().Unix()-atomic.LoadInt64(&c.lastSent))
			if time.Now().Unix()-atomic.LoadInt64(&c.lastSent) >= c.options.KeepAlive || time.Now().Unix()-atomic.LoadInt64(&c.lastReceived) >= c.options.KeepAlive {
				if atomic.LoadInt32(&c.pingOutstanding) == 0 {
					DEBUG.Println(PNG, "keepalive sending ping")
					ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
					//We don't want to wait behind large messages being sent, the Write call
					//will block until it it able to send the packet.
					atomic.StoreInt32(&c.pingOutstanding, 1)
					ping.Write(c.conn)
					atomic.StoreInt64(&c.lastSent, time.Now().Unix())
					pingSent = time.Now()
				}
			}
			if atomic.LoadInt32(&c.pingOutstanding) > 0 && time.Now().Sub(pingSent) >= c.options.PingTimeout {
				CRITICAL.Println(PNG, "pingresp not received, disconnecting")
				c.errors <- errors.New("pingresp not received, disconnecting")
				return
			}
		}
	}
}
