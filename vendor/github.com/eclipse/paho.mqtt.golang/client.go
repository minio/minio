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

// Portions copyright © 2018 TIBCO Software Inc.

// Package mqtt provides an MQTT v3.1.1 client library.
package mqtt

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	disconnected uint32 = iota
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
	// IsConnected returns a bool signifying whether
	// the client is connected or not.
	IsConnected() bool
	// IsConnectionOpen return a bool signifying wether the client has an active
	// connection to mqtt broker, i.e not in disconnected or reconnect mode
	IsConnectionOpen() bool
	// Connect will create a connection to the message broker, by default
	// it will attempt to connect at v3.1.1 and auto retry at v3.1 if that
	// fails
	Connect() Token
	// Disconnect will end the connection with the server, but not before waiting
	// the specified number of milliseconds to wait for existing work to be
	// completed.
	Disconnect(quiesce uint)
	// Publish will publish a message with the specified QoS and content
	// to the specified topic.
	// Returns a token to track delivery of the message to the broker
	Publish(topic string, qos byte, retained bool, payload interface{}) Token
	// Subscribe starts a new subscription. Provide a MessageHandler to be executed when
	// a message is published on the topic provided, or nil for the default handler
	Subscribe(topic string, qos byte, callback MessageHandler) Token
	// SubscribeMultiple starts a new subscription for multiple topics. Provide a MessageHandler to
	// be executed when a message is published on one of the topics provided, or nil for the
	// default handler
	SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token
	// Unsubscribe will end the subscription from each of the topics provided.
	// Messages published to those topics from other clients will no longer be
	// received.
	Unsubscribe(topics ...string) Token
	// AddRoute allows you to add a handler for messages on a specific topic
	// without making a subscription. For example having a different handler
	// for parts of a wildcard subscription
	AddRoute(topic string, callback MessageHandler)
	// OptionsReader returns a ClientOptionsReader which is a copy of the clientoptions
	// in use by the client.
	OptionsReader() ClientOptionsReader
}

// client implements the Client interface
type client struct {
	lastSent        int64
	lastReceived    int64
	pingOutstanding int32
	status          uint32
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
	case 0x83, 0x84:
		c.options.protocolVersionExplicit = true
	default:
		c.options.ProtocolVersion = 4
		c.options.protocolVersionExplicit = false
	}
	c.persist = c.options.Store
	c.status = disconnected
	c.messageIds = messageIds{index: make(map[uint16]tokenCompletor)}
	c.msgRouter, c.stopRouter = newRouter()
	c.msgRouter.setDefaultHandler(c.options.DefaultPublishHandler)
	if !c.options.AutoReconnect {
		c.options.MessageChannelDepth = 0
	}
	return c
}

// AddRoute allows you to add a handler for messages on a specific topic
// without making a subscription. For example having a different handler
// for parts of a wildcard subscription
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
	status := atomic.LoadUint32(&c.status)
	switch {
	case status == connected:
		return true
	case c.options.AutoReconnect && status > connecting:
		return true
	default:
		return false
	}
}

// IsConnectionOpen return a bool signifying whether the client has an active
// connection to mqtt broker, i.e not in disconnected or reconnect mode
func (c *client) IsConnectionOpen() bool {
	c.RLock()
	defer c.RUnlock()
	status := atomic.LoadUint32(&c.status)
	switch {
	case status == connected:
		return true
	default:
		return false
	}
}

func (c *client) connectionStatus() uint32 {
	c.RLock()
	defer c.RUnlock()
	status := atomic.LoadUint32(&c.status)
	return status
}

func (c *client) setConnected(status uint32) {
	c.Lock()
	defer c.Unlock()
	atomic.StoreUint32(&c.status, uint32(status))
}

//ErrNotConnected is the error returned from function calls that are
//made when the client is not connected to a broker
var ErrNotConnected = errors.New("Not Connected")

// Connect will create a connection to the message broker, by default
// it will attempt to connect at v3.1.1 and auto retry at v3.1 if that
// fails
func (c *client) Connect() Token {
	var err error
	t := newToken(packets.Connect).(*ConnectToken)
	DEBUG.Println(CLI, "Connect()")

	c.obound = make(chan *PacketAndToken, c.options.MessageChannelDepth)
	c.oboundP = make(chan *PacketAndToken, c.options.MessageChannelDepth)
	c.ibound = make(chan packets.ControlPacket)

	go func() {
		c.persist.Open()

		c.setConnected(connecting)
		c.errors = make(chan error, 1)
		c.stop = make(chan struct{})

		var rc byte
		protocolVersion := c.options.ProtocolVersion

		if len(c.options.Servers) == 0 {
			t.setError(fmt.Errorf("No servers defined to connect to"))
			return
		}

		for _, broker := range c.options.Servers {
			cm := newConnectMsgFromOptions(&c.options, broker)
			c.options.ProtocolVersion = protocolVersion
		CONN:
			DEBUG.Println(CLI, "about to write new connect msg")
			c.conn, err = openConnection(broker, c.options.TLSConfig, c.options.ConnectTimeout, c.options.HTTPHeaders)
			if err == nil {
				DEBUG.Println(CLI, "socket connected to broker")
				switch c.options.ProtocolVersion {
				case 3:
					DEBUG.Println(CLI, "Using MQTT 3.1 protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 3
				case 0x83:
					DEBUG.Println(CLI, "Using MQTT 3.1b protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 0x83
				case 0x84:
					DEBUG.Println(CLI, "Using MQTT 3.1.1b protocol")
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 0x84
				default:
					DEBUG.Println(CLI, "Using MQTT 3.1.1 protocol")
					c.options.ProtocolVersion = 4
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 4
				}
				cm.Write(c.conn)

				rc, t.sessionPresent = c.connect()
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
			c.setConnected(disconnected)
			c.persist.Close()
			t.returnCode = rc
			if rc != packets.ErrNetworkError {
				t.setError(packets.ConnErrors[rc])
			} else {
				t.setError(fmt.Errorf("%s : %s", packets.ConnErrors[rc], err))
			}
			return
		}

		c.options.protocolVersionExplicit = true

		if c.options.KeepAlive != 0 {
			atomic.StoreInt32(&c.pingOutstanding, 0)
			atomic.StoreInt64(&c.lastReceived, time.Now().Unix())
			atomic.StoreInt64(&c.lastSent, time.Now().Unix())
			c.workers.Add(1)
			go keepalive(c)
		}

		c.incomingPubChan = make(chan *packets.PublishPacket, c.options.MessageChannelDepth)
		c.msgRouter.matchAndDispatch(c.incomingPubChan, c.options.Order, c)

		c.setConnected(connected)
		DEBUG.Println(CLI, "client is connected")
		if c.options.OnConnect != nil {
			go c.options.OnConnect(c)
		}

		c.workers.Add(4)
		go errorWatch(c)
		go alllogic(c)
		go outgoing(c)
		go incoming(c)

		// Take care of any messages in the store
		if c.options.CleanSession == false {
			c.resume(c.options.ResumeSubs)
		} else {
			c.persist.Reset()
		}

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
		for _, broker := range c.options.Servers {
			cm := newConnectMsgFromOptions(&c.options, broker)
			DEBUG.Println(CLI, "about to write new connect msg")
			c.conn, err = openConnection(broker, c.options.TLSConfig, c.options.ConnectTimeout, c.options.HTTPHeaders)
			if err == nil {
				DEBUG.Println(CLI, "socket connected to broker")
				switch c.options.ProtocolVersion {
				case 0x83:
					DEBUG.Println(CLI, "Using MQTT 3.1b protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 0x83
				case 0x84:
					DEBUG.Println(CLI, "Using MQTT 3.1.1b protocol")
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 0x84
				case 3:
					DEBUG.Println(CLI, "Using MQTT 3.1 protocol")
					cm.ProtocolName = "MQIsdp"
					cm.ProtocolVersion = 3
				default:
					DEBUG.Println(CLI, "Using MQTT 3.1.1 protocol")
					cm.ProtocolName = "MQTT"
					cm.ProtocolVersion = 4
				}
				cm.Write(c.conn)

				rc, _ = c.connect()
				if rc != packets.Accepted {
					c.conn.Close()
					c.conn = nil
					//if the protocol version was explicitly set don't do any fallback
					if c.options.protocolVersionExplicit {
						ERROR.Println(CLI, "Connecting to", broker, "CONNACK was not Accepted, but rather", packets.ConnackReturnCodes[rc])
						continue
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
	if c.connectionStatus() == disconnected {
		DEBUG.Println(CLI, "Client moved to disconnected state while reconnecting, abandoning reconnect")
		return
	}

	c.stop = make(chan struct{})

	if c.options.KeepAlive != 0 {
		atomic.StoreInt32(&c.pingOutstanding, 0)
		atomic.StoreInt64(&c.lastReceived, time.Now().Unix())
		atomic.StoreInt64(&c.lastSent, time.Now().Unix())
		c.workers.Add(1)
		go keepalive(c)
	}

	c.setConnected(connected)
	DEBUG.Println(CLI, "client is reconnected")
	if c.options.OnConnect != nil {
		go c.options.OnConnect(c)
	}

	c.workers.Add(4)
	go errorWatch(c)
	go alllogic(c)
	go outgoing(c)
	go incoming(c)

	c.resume(false)
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func (c *client) connect() (byte, bool) {
	DEBUG.Println(NET, "connect started")

	ca, err := packets.ReadPacket(c.conn)
	if err != nil {
		ERROR.Println(NET, "connect got error", err)
		return packets.ErrNetworkError, false
	}
	if ca == nil {
		ERROR.Println(NET, "received nil packet")
		return packets.ErrNetworkError, false
	}

	msg, ok := ca.(*packets.ConnackPacket)
	if !ok {
		ERROR.Println(NET, "received msg that was not CONNACK")
		return packets.ErrNetworkError, false
	}

	DEBUG.Println(NET, "received connack")
	return msg.ReturnCode, msg.SessionPresent
}

// Disconnect will end the connection with the server, but not before waiting
// the specified number of milliseconds to wait for existing work to be
// completed.
func (c *client) Disconnect(quiesce uint) {
	status := atomic.LoadUint32(&c.status)
	if status == connected {
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
		return
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
		if c.stop != nil {
			close(c.stop)
		}
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
	c.messageIds.cleanUp()
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

	if pub.Qos != 0 && pub.MessageID == 0 {
		pub.MessageID = c.getID(token)
		token.messageID = pub.MessageID
	}
	persistOutbound(c.persist, pub)
	if c.connectionStatus() == reconnecting {
		DEBUG.Println(CLI, "storing publish message (reconnecting), topic:", topic)
	} else {
		DEBUG.Println(CLI, "sending publish message, topic:", topic)
		c.obound <- &PacketAndToken{p: pub, t: token}
	}
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

	if strings.HasPrefix(topic, "$share") {
		topic = strings.Join(strings.Split(topic, "/")[2:], "/")
	}

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

// Load all stored messages and resend them
// Call this to ensure QOS > 1,2 even after an application crash
func (c *client) resume(subscription bool) {

	storedKeys := c.persist.All()
	for _, key := range storedKeys {
		packet := c.persist.Get(key)
		details := packet.Details()
		if isKeyOutbound(key) {
			switch packet.(type) {
			case *packets.SubscribePacket:
				if subscription {
					DEBUG.Println(STR, fmt.Sprintf("loaded pending subscribe (%d)", details.MessageID))
					token := newToken(packets.Subscribe).(*SubscribeToken)
					c.oboundP <- &PacketAndToken{p: packet, t: token}
				}
			case *packets.UnsubscribePacket:
				if subscription {
					DEBUG.Println(STR, fmt.Sprintf("loaded pending unsubscribe (%d)", details.MessageID))
					token := newToken(packets.Unsubscribe).(*UnsubscribeToken)
					c.oboundP <- &PacketAndToken{p: packet, t: token}
				}
			case *packets.PubrelPacket:
				DEBUG.Println(STR, fmt.Sprintf("loaded pending pubrel (%d)", details.MessageID))
				select {
				case c.oboundP <- &PacketAndToken{p: packet, t: nil}:
				case <-c.stop:
				}
			case *packets.PublishPacket:
				token := newToken(packets.Publish).(*PublishToken)
				token.messageID = details.MessageID
				c.claimID(token, details.MessageID)
				DEBUG.Println(STR, fmt.Sprintf("loaded pending publish (%d)", details.MessageID))
				DEBUG.Println(STR, details)
				c.obound <- &PacketAndToken{p: packet, t: token}
			default:
				ERROR.Println(STR, "invalid message type in store (discarded)")
				c.persist.Del(key)
			}
		} else {
			switch packet.(type) {
			case *packets.PubrelPacket, *packets.PublishPacket:
				DEBUG.Println(STR, fmt.Sprintf("loaded pending incomming (%d)", details.MessageID))
				select {
				case c.ibound <- packet:
				case <-c.stop:
				}
			default:
				ERROR.Println(STR, "invalid message type in store (discarded)")
				c.persist.Del(key)
			}
		}
	}
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

// OptionsReader returns a ClientOptionsReader which is a copy of the clientoptions
// in use by the client.
func (c *client) OptionsReader() ClientOptionsReader {
	r := ClientOptionsReader{options: &c.options}
	return r
}

//DefaultConnectionLostHandler is a definition of a function that simply
//reports to the DEBUG log the reason for the client losing a connection.
func DefaultConnectionLostHandler(client Client, reason error) {
	DEBUG.Println("Connection lost:", reason.Error())
}
