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

// Package mqtt provides an MQTT v3.1.1 client library.
package mqtt

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type connStatus uint

const (
	disconnected connStatus = iota
	connecting
	reconnecting
	connected
)

// Client is the interface definition for a Client as used by this
// library, the interface is primarily to allow mocking tests.
//
// It is an MQTT v3.1.1 client for communicating
// with an MQTT server using non-blocking methods that allow work
// to be done in the background.
// An application may connect to an MQTT server using:
//   A plain TCP socket
//   A secure SSL/TLS socket
//   A websocket
// To enable ensured message delivery at Quality of Service (QoS) levels
// described in the MQTT spec, a message persistence mechanism must be
// used. This is done by providing a type which implements the Store
// interface. For convenience, FileStore and MemoryStore are provided
// implementations that should be sufficient for most use cases. More
// information can be found in their respective documentation.
// Numerous connection options may be specified by configuring a
// and then supplying a ClientOptions type.
type Client interface {
	IsConnected() bool
	Connect() Token
	Disconnect(quiesce uint)
	Publish(topic string, qos byte, retained bool, payload interface{}) Token
	Subscribe(topic string, qos byte, callback MessageHandler) Token
	SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token
	Unsubscribe(topics ...string) Token
	AddRoute(topic string, callback MessageHandler)
}

// client implements the Client interface
type client struct {
	sync.RWMutex
	messageIds
	conn            net.Conn
	ibound          chan packets.ControlPacket
	obound          chan *PacketAndToken
	oboundP         chan *PacketAndToken
	msgRouter       *router
	stopRouter      chan bool
	incomingPubChan chan *packets.PublishPacket
	errors          chan error
	stop            chan struct{}
	persist         Store
	options         ClientOptions
	pingResp        chan struct{}
	packetResp      chan struct{}
	keepaliveReset  chan struct{}
	status          connStatus
	workers         sync.WaitGroup
}

// NewClient will create an MQTT v3.1.1 client with all of the options specified
// in the provided ClientOptions. The client must have the Connect method called
// on it before it may be used. This is to make sure resources (such as a net
// connection) are created before the application is actually ready.
func NewClient(o *ClientOptions) Client {
	c := &client{}
	c.options = *o

	if c.options.Store == nil {
		c.options.Store = NewMemoryStore()
	}
	switch c.options.ProtocolVersion {
	case 3, 4:
		c.options.protocolVersionExplicit = true
	default:
		c.options.ProtocolVersion = 4
		c.options.protocolVersionExplicit = false
	}
	c.persist = c.options.Store
	c.status = disconnected
	c.messageIds = messageIds{index: make(map[uint16]Token)}
	c.msgRouter, c.stopRouter = newRouter()
	c.msgRouter.setDefaultHandler(c.options.DefaultPublishHander)
	if !c.options.AutoReconnect {
		c.options.MessageChannelDepth = 0
	}
	return c
}

func (c *client) AddRoute(topic string, callback MessageHandler) {
	if callback != nil {
		c.msgRouter.addRoute(topic, callback)
	}
}

// IsConnected returns a bool signifying whether
// the client is connected or not.
func (c *client) IsConnected() bool {
	c.RLock()
	defer c.RUnlock()
	switch {
	case c.status == connected:
		return true
	case c.options.AutoReconnect && c.status > disconnected:
		return true
	default:
		return false
	}
}

func (c *client) connectionStatus() connStatus {
	c.RLock()
	defer c.RUnlock()
	return c.status
}

func (c *client) setConnected(status connStatus) {
	c.Lock()
	defer c.Unlock()
	c.status = status
}

//ErrNotConnected is the error returned from function calls that are
//made when the client is not connected to a broker
var ErrNotConnected = errors.New("Not Connected")

// Connect will create a connection to the message broker
// If clean session is false, then a slice will
// be returned containing Receipts for all messages
// that were in-flight at the last disconnect.
// If clean session is true, then any existing client
// state will be removed.
func (c *client) Connect() Token {
	var err error
	t := newToken(packets.Connect).(*ConnectToken)
	DEBUG.Println(CLI, "Connect()")

	go func() {
		c.persist.Open()

		c.setConnected(connecting)
		var rc byte
		cm := newConnectMsgFromOptions(&c.options)

		for _, broker := range c.options.Servers {
		CONN:
			DEBUG.Println(CLI, "about to write new connect msg")
			c.conn, err = openConnection(broker, &c.options.TLSConfig, c.options.ConnectTimeout)
			if err == nil {
				DEBUG.Println(CLI, "socket connected to broker")
				switch c.options.ProtocolVersion {
				case 3:
					DEBUG.Println(CLI, "Using MQTT 3.1 protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 3
				default:
					DEBUG.Println(CLI, "Using MQTT 3.1.1 protocol")
					c.options.ProtocolVersion = 4
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 4
				}
				cm.Write(c.conn)

				rc = c.connect()
				if rc != packets.Accepted {
					if c.conn != nil {
						c.conn.Close()
						c.conn = nil
					}
					//if the protocol version was explicitly set don't do any fallback
					if c.options.protocolVersionExplicit {
						ERROR.Println(CLI, "Connecting to", broker, "CONNACK was not CONN_ACCEPTED, but rather", packets.ConnackReturnCodes[rc])
						continue
					}
					if c.options.ProtocolVersion == 4 {
						DEBUG.Println(CLI, "Trying reconnect using MQTT 3.1 protocol")
						c.options.ProtocolVersion = 3
						goto CONN
					}
				}
				break
			} else {
				ERROR.Println(CLI, err.Error())
				WARN.Println(CLI, "failed to connect to broker, trying next")
				rc = packets.ErrNetworkError
			}
		}

		if c.conn == nil {
			ERROR.Println(CLI, "Failed to connect to a broker")
			t.returnCode = rc
			if rc != packets.ErrNetworkError {
				t.err = packets.ConnErrors[rc]
			} else {
				t.err = fmt.Errorf("%s : %s", packets.ConnErrors[rc], err)
			}
			c.setConnected(disconnected)
			c.persist.Close()
			t.flowComplete()
			return
		}

		if c.options.KeepAlive != 0 {
			c.workers.Add(1)
			go keepalive(c)
		}

		c.obound = make(chan *PacketAndToken, c.options.MessageChannelDepth)
		c.oboundP = make(chan *PacketAndToken, c.options.MessageChannelDepth)
		c.ibound = make(chan packets.ControlPacket)
		c.errors = make(chan error, 1)
		c.stop = make(chan struct{})
		c.pingResp = make(chan struct{}, 1)
		c.packetResp = make(chan struct{}, 1)
		c.keepaliveReset = make(chan struct{}, 1)

		c.incomingPubChan = make(chan *packets.PublishPacket, c.options.MessageChannelDepth)
		c.msgRouter.matchAndDispatch(c.incomingPubChan, c.options.Order, c)

		c.setConnected(connected)
		DEBUG.Println(CLI, "client is connected")
		if c.options.OnConnect != nil {
			go c.options.OnConnect(c)
		}

		// Take care of any messages in the store
		//var leftovers []Receipt
		if c.options.CleanSession == false {
			//leftovers = c.resume()
		} else {
			c.persist.Reset()
		}

		go errorWatch(c)

		// Do not start incoming until resume has completed
		c.workers.Add(3)
		go alllogic(c)
		go outgoing(c)
		go incoming(c)

		DEBUG.Println(CLI, "exit startClient")
		t.flowComplete()
	}()
	return t
}

// internal function used to reconnect the client when it loses its connection
func (c *client) reconnect() {
	DEBUG.Println(CLI, "enter reconnect")
	var (
		err error

		rc    = byte(1)
		sleep = time.Duration(1 * time.Second)
	)

	for rc != 0 && c.status != disconnected {
		cm := newConnectMsgFromOptions(&c.options)

		for _, broker := range c.options.Servers {
		CONN:
			DEBUG.Println(CLI, "about to write new connect msg")
			c.conn, err = openConnection(broker, &c.options.TLSConfig, c.options.ConnectTimeout)
			if err == nil {
				DEBUG.Println(CLI, "socket connected to broker")
				switch c.options.ProtocolVersion {
				case 3:
					DEBUG.Println(CLI, "Using MQTT 3.1 protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 3
				default:
					DEBUG.Println(CLI, "Using MQTT 3.1.1 protocol")
					c.options.ProtocolVersion = 4
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 4
				}
				cm.Write(c.conn)

				rc = c.connect()
				if rc != packets.Accepted {
					c.conn.Close()
					c.conn = nil
					//if the protocol version was explicitly set don't do any fallback
					if c.options.protocolVersionExplicit {
						ERROR.Println(CLI, "Connecting to", broker, "CONNACK was not Accepted, but rather", packets.ConnackReturnCodes[rc])
						continue
					}
					if c.options.ProtocolVersion == 4 {
						DEBUG.Println(CLI, "Trying reconnect using MQTT 3.1 protocol")
						c.options.ProtocolVersion = 3
						goto CONN
					}
				}
				break
			} else {
				ERROR.Println(CLI, err.Error())
				WARN.Println(CLI, "failed to connect to broker, trying next")
				rc = packets.ErrNetworkError
			}
		}
		if rc != 0 {
			DEBUG.Println(CLI, "Reconnect failed, sleeping for", int(sleep.Seconds()), "seconds")
			time.Sleep(sleep)
			if sleep < c.options.MaxReconnectInterval {
				sleep *= 2
			}

			if sleep > c.options.MaxReconnectInterval {
				sleep = c.options.MaxReconnectInterval
			}
		}
	}
	// Disconnect() must have been called while we were trying to reconnect.
	if c.status == disconnected {
		DEBUG.Println(CLI, "Client moved to disconnected state while reconnecting, abandoning reconnect")
		return
	}

	if c.options.KeepAlive != 0 {
		c.workers.Add(1)
		go keepalive(c)
	}

	c.stop = make(chan struct{})

	c.setConnected(connected)
	DEBUG.Println(CLI, "client is reconnected")
	if c.options.OnConnect != nil {
		go c.options.OnConnect(c)
	}

	go errorWatch(c)

	c.workers.Add(3)
	go alllogic(c)
	go outgoing(c)
	go incoming(c)
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func (c *client) connect() byte {
	DEBUG.Println(NET, "connect started")

	ca, err := packets.ReadPacket(c.conn)
	if err != nil {
		ERROR.Println(NET, "connect got error", err)
		return packets.ErrNetworkError
	}
	if ca == nil {
		ERROR.Println(NET, "received nil packet")
		return packets.ErrNetworkError
	}

	msg, ok := ca.(*packets.ConnackPacket)
	if !ok {
		ERROR.Println(NET, "received msg that was not CONNACK")
		return packets.ErrNetworkError
	}

	DEBUG.Println(NET, "received connack")
	return msg.ReturnCode
}

// Disconnect will end the connection with the server, but not before waiting
// the specified number of milliseconds to wait for existing work to be
// completed.
func (c *client) Disconnect(quiesce uint) {
	if c.status == connected {
		DEBUG.Println(CLI, "disconnecting")
		c.setConnected(disconnected)

		dm := packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
		dt := newToken(packets.Disconnect)
		c.oboundP <- &PacketAndToken{p: dm, t: dt}

		// wait for work to finish, or quiesce time consumed
		dt.WaitTimeout(time.Duration(quiesce) * time.Millisecond)
	} else {
		WARN.Println(CLI, "Disconnect() called but not connected (disconnected/reconnecting)")
		c.setConnected(disconnected)
	}

	c.disconnect()
}

// ForceDisconnect will end the connection with the mqtt broker immediately.
func (c *client) forceDisconnect() {
	if !c.IsConnected() {
		WARN.Println(CLI, "already disconnected")
		return
	}
	c.setConnected(disconnected)
	c.conn.Close()
	DEBUG.Println(CLI, "forcefully disconnecting")
	c.disconnect()
}

func (c *client) internalConnLost(err error) {
	// Only do anything if this was called and we are still "connected"
	// forceDisconnect can cause incoming/outgoing/alllogic to end with
	// error from closing the socket but state will be "disconnected"
	if c.IsConnected() {
		c.closeStop()
		c.conn.Close()
		c.workers.Wait()
		if c.options.CleanSession {
			c.messageIds.cleanUp()
		}
		if c.options.AutoReconnect {
			c.setConnected(reconnecting)
			go c.reconnect()
		} else {
			c.setConnected(disconnected)
		}
		if c.options.OnConnectionLost != nil {
			go c.options.OnConnectionLost(c, err)
		}
	}
}

func (c *client) closeStop() {
	c.Lock()
	defer c.Unlock()
	select {
	case <-c.stop:
		DEBUG.Println("In disconnect and stop channel is already closed")
	default:
		close(c.stop)
	}
}

func (c *client) closeConn() {
	c.Lock()
	defer c.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *client) disconnect() {
	c.closeStop()
	c.closeConn()
	c.workers.Wait()
	close(c.stopRouter)
	DEBUG.Println(CLI, "disconnected")
	c.persist.Close()
}

// Publish will publish a message with the specified QoS and content
// to the specified topic.
// Returns a token to track delivery of the message to the broker
func (c *client) Publish(topic string, qos byte, retained bool, payload interface{}) Token {
	token := newToken(packets.Publish).(*PublishToken)
	DEBUG.Println(CLI, "enter Publish")
	switch {
	case !c.IsConnected():
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	case c.connectionStatus() == reconnecting && qos == 0:
		token.flowComplete()
		return token
	}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = qos
	pub.TopicName = topic
	pub.Retain = retained
	switch payload.(type) {
	case string:
		pub.Payload = []byte(payload.(string))
	case []byte:
		pub.Payload = payload.([]byte)
	default:
		token.err = errors.New("Unknown payload type")
		token.flowComplete()
		return token
	}

	DEBUG.Println(CLI, "sending publish message, topic:", topic)
	if pub.Qos != 0 && pub.MessageID == 0 {
		pub.MessageID = c.getID(token)
		token.messageID = pub.MessageID
	}
	persistOutbound(c.persist, pub)
	c.obound <- &PacketAndToken{p: pub, t: token}
	return token
}

// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
// a message is published on the topic provided.
func (c *client) Subscribe(topic string, qos byte, callback MessageHandler) Token {
	token := newToken(packets.Subscribe).(*SubscribeToken)
	DEBUG.Println(CLI, "enter Subscribe")
	if !c.IsConnected() {
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	if err := validateTopicAndQos(topic, qos); err != nil {
		token.err = err
		return token
	}
	sub.Topics = append(sub.Topics, topic)
	sub.Qoss = append(sub.Qoss, qos)
	DEBUG.Println(CLI, sub.String())

	if callback != nil {
		c.msgRouter.addRoute(topic, callback)
	}

	token.subs = append(token.subs, topic)
	c.oboundP <- &PacketAndToken{p: sub, t: token}
	DEBUG.Println(CLI, "exit Subscribe")
	return token
}

// SubscribeMultiple starts a new subscription for multiple topics. Provide a MessageHandler to
// be executed when a message is published on one of the topics provided.
func (c *client) SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token {
	var err error
	token := newToken(packets.Subscribe).(*SubscribeToken)
	DEBUG.Println(CLI, "enter SubscribeMultiple")
	if !c.IsConnected() {
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	if sub.Topics, sub.Qoss, err = validateSubscribeMap(filters); err != nil {
		token.err = err
		return token
	}

	if callback != nil {
		for topic := range filters {
			c.msgRouter.addRoute(topic, callback)
		}
	}
	token.subs = make([]string, len(sub.Topics))
	copy(token.subs, sub.Topics)
	c.oboundP <- &PacketAndToken{p: sub, t: token}
	DEBUG.Println(CLI, "exit SubscribeMultiple")
	return token
}

// Unsubscribe will end the subscription from each of the topics provided.
// Messages published to those topics from other clients will no longer be
// received.
func (c *client) Unsubscribe(topics ...string) Token {
	token := newToken(packets.Unsubscribe).(*UnsubscribeToken)
	DEBUG.Println(CLI, "enter Unsubscribe")
	if !c.IsConnected() {
		token.err = ErrNotConnected
		token.flowComplete()
		return token
	}
	unsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	unsub.Topics = make([]string, len(topics))
	copy(unsub.Topics, topics)

	c.oboundP <- &PacketAndToken{p: unsub, t: token}
	for _, topic := range topics {
		c.msgRouter.deleteRoute(topic)
	}

	DEBUG.Println(CLI, "exit Unsubscribe")
	return token
}

func (c *client) OptionsReader() ClientOptionsReader {
	r := ClientOptionsReader{options: &c.options}
	return r
}

//DefaultConnectionLostHandler is a definition of a function that simply
//reports to the DEBUG log the reason for the client losing a connection.
func DefaultConnectionLostHandler(client Client, reason error) {
	DEBUG.Println("Connection lost:", reason.Error())
}
