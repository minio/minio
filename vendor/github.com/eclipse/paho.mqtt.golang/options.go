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

package mqtt

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CredentialsProvider allows the username and password to be updated
// before reconnecting. It should return the current username and password.
type CredentialsProvider func() (username string, password string)

// MessageHandler is a callback type which can be set to be
// executed upon the arrival of messages published to topics
// to which the client is subscribed.
type MessageHandler func(Client, Message)

// ConnectionLostHandler is a callback type which can be set to be
// executed upon an unintended disconnection from the MQTT broker.
// Disconnects caused by calling Disconnect or ForceDisconnect will
// not cause an OnConnectionLost callback to execute.
type ConnectionLostHandler func(Client, error)

// OnConnectHandler is a callback that is called when the client
// state changes from unconnected/disconnected to connected. Both
// at initial connection and on reconnection
type OnConnectHandler func(Client)

// ClientOptions contains configurable options for an Client.
type ClientOptions struct {
	Servers                 []*url.URL
	ClientID                string
	Username                string
	Password                string
	CredentialsProvider     CredentialsProvider
	CleanSession            bool
	Order                   bool
	WillEnabled             bool
	WillTopic               string
	WillPayload             []byte
	WillQos                 byte
	WillRetained            bool
	ProtocolVersion         uint
	protocolVersionExplicit bool
	TLSConfig               *tls.Config
	KeepAlive               int64
	PingTimeout             time.Duration
	ConnectTimeout          time.Duration
	MaxReconnectInterval    time.Duration
	AutoReconnect           bool
	Store                   Store
	DefaultPublishHandler   MessageHandler
	OnConnect               OnConnectHandler
	OnConnectionLost        ConnectionLostHandler
	WriteTimeout            time.Duration
	MessageChannelDepth     uint
	ResumeSubs              bool
	HTTPHeaders             http.Header
}

// NewClientOptions will create a new ClientClientOptions type with some
// default values.
//   Port: 1883
//   CleanSession: True
//   Order: True
//   KeepAlive: 30 (seconds)
//   ConnectTimeout: 30 (seconds)
//   MaxReconnectInterval 10 (minutes)
//   AutoReconnect: True
func NewClientOptions() *ClientOptions {
	o := &ClientOptions{
		Servers:                 nil,
		ClientID:                "",
		Username:                "",
		Password:                "",
		CleanSession:            true,
		Order:                   true,
		WillEnabled:             false,
		WillTopic:               "",
		WillPayload:             nil,
		WillQos:                 0,
		WillRetained:            false,
		ProtocolVersion:         0,
		protocolVersionExplicit: false,
		KeepAlive:               30,
		PingTimeout:             10 * time.Second,
		ConnectTimeout:          30 * time.Second,
		MaxReconnectInterval:    10 * time.Minute,
		AutoReconnect:           true,
		Store:                   nil,
		OnConnect:               nil,
		OnConnectionLost:        DefaultConnectionLostHandler,
		WriteTimeout:            0, // 0 represents timeout disabled
		MessageChannelDepth:     100,
		ResumeSubs:              false,
		HTTPHeaders:             make(map[string][]string),
	}
	return o
}

// AddBroker adds a broker URI to the list of brokers to be used. The format should be
// scheme://host:port
// Where "scheme" is one of "tcp", "ssl", or "ws", "host" is the ip-address (or hostname)
// and "port" is the port on which the broker is accepting connections.
//
// Default values for hostname is "127.0.0.1", for schema is "tcp://".
//
// An example broker URI would look like: tcp://foobar.com:1883
func (o *ClientOptions) AddBroker(server string) *ClientOptions {
	if len(server) > 0 && server[0] == ':' {
		server = "127.0.0.1" + server
	}
	if !strings.Contains(server, "://") {
		server = "tcp://" + server
	}
	brokerURI, err := url.Parse(server)
	if err != nil {
		ERROR.Println(CLI, "Failed to parse %q broker address: %s", server, err)
		return o
	}
	o.Servers = append(o.Servers, brokerURI)
	return o
}

// SetResumeSubs will enable resuming of stored (un)subscribe messages when connecting
// but not reconnecting if CleanSession is false. Otherwise these messages are discarded.
func (o *ClientOptions) SetResumeSubs(resume bool) *ClientOptions {
	o.ResumeSubs = resume
	return o
}

// SetClientID will set the client id to be used by this client when
// connecting to the MQTT broker. According to the MQTT v3.1 specification,
// a client id mus be no longer than 23 characters.
func (o *ClientOptions) SetClientID(id string) *ClientOptions {
	o.ClientID = id
	return o
}

// SetUsername will set the username to be used by this client when connecting
// to the MQTT broker. Note: without the use of SSL/TLS, this information will
// be sent in plaintext accross the wire.
func (o *ClientOptions) SetUsername(u string) *ClientOptions {
	o.Username = u
	return o
}

// SetPassword will set the password to be used by this client when connecting
// to the MQTT broker. Note: without the use of SSL/TLS, this information will
// be sent in plaintext accross the wire.
func (o *ClientOptions) SetPassword(p string) *ClientOptions {
	o.Password = p
	return o
}

// SetCredentialsProvider will set a method to be called by this client when
// connecting to the MQTT broker that provide the current username and password.
// Note: without the use of SSL/TLS, this information will be sent
// in plaintext accross the wire.
func (o *ClientOptions) SetCredentialsProvider(p CredentialsProvider) *ClientOptions {
	o.CredentialsProvider = p
	return o
}

// SetCleanSession will set the "clean session" flag in the connect message
// when this client connects to an MQTT broker. By setting this flag, you are
// indicating that no messages saved by the broker for this client should be
// delivered. Any messages that were going to be sent by this client before
// diconnecting previously but didn't will not be sent upon connecting to the
// broker.
func (o *ClientOptions) SetCleanSession(clean bool) *ClientOptions {
	o.CleanSession = clean
	return o
}

// SetOrderMatters will set the message routing to guarantee order within
// each QoS level. By default, this value is true. If set to false,
// this flag indicates that messages can be delivered asynchronously
// from the client to the application and possibly arrive out of order.
func (o *ClientOptions) SetOrderMatters(order bool) *ClientOptions {
	o.Order = order
	return o
}

// SetTLSConfig will set an SSL/TLS configuration to be used when connecting
// to an MQTT broker. Please read the official Go documentation for more
// information.
func (o *ClientOptions) SetTLSConfig(t *tls.Config) *ClientOptions {
	o.TLSConfig = t
	return o
}

// SetStore will set the implementation of the Store interface
// used to provide message persistence in cases where QoS levels
// QoS_ONE or QoS_TWO are used. If no store is provided, then the
// client will use MemoryStore by default.
func (o *ClientOptions) SetStore(s Store) *ClientOptions {
	o.Store = s
	return o
}

// SetKeepAlive will set the amount of time (in seconds) that the client
// should wait before sending a PING request to the broker. This will
// allow the client to know that a connection has not been lost with the
// server.
func (o *ClientOptions) SetKeepAlive(k time.Duration) *ClientOptions {
	o.KeepAlive = int64(k / time.Second)
	return o
}

// SetPingTimeout will set the amount of time (in seconds) that the client
// will wait after sending a PING request to the broker, before deciding
// that the connection has been lost. Default is 10 seconds.
func (o *ClientOptions) SetPingTimeout(k time.Duration) *ClientOptions {
	o.PingTimeout = k
	return o
}

// SetProtocolVersion sets the MQTT version to be used to connect to the
// broker. Legitimate values are currently 3 - MQTT 3.1 or 4 - MQTT 3.1.1
func (o *ClientOptions) SetProtocolVersion(pv uint) *ClientOptions {
	if (pv >= 3 && pv <= 4) || (pv > 0x80) {
		o.ProtocolVersion = pv
		o.protocolVersionExplicit = true
	}
	return o
}

// UnsetWill will cause any set will message to be disregarded.
func (o *ClientOptions) UnsetWill() *ClientOptions {
	o.WillEnabled = false
	return o
}

// SetWill accepts a string will message to be set. When the client connects,
// it will give this will message to the broker, which will then publish the
// provided payload (the will) to any clients that are subscribed to the provided
// topic.
func (o *ClientOptions) SetWill(topic string, payload string, qos byte, retained bool) *ClientOptions {
	o.SetBinaryWill(topic, []byte(payload), qos, retained)
	return o
}

// SetBinaryWill accepts a []byte will message to be set. When the client connects,
// it will give this will message to the broker, which will then publish the
// provided payload (the will) to any clients that are subscribed to the provided
// topic.
func (o *ClientOptions) SetBinaryWill(topic string, payload []byte, qos byte, retained bool) *ClientOptions {
	o.WillEnabled = true
	o.WillTopic = topic
	o.WillPayload = payload
	o.WillQos = qos
	o.WillRetained = retained
	return o
}

// SetDefaultPublishHandler sets the MessageHandler that will be called when a message
// is received that does not match any known subscriptions.
func (o *ClientOptions) SetDefaultPublishHandler(defaultHandler MessageHandler) *ClientOptions {
	o.DefaultPublishHandler = defaultHandler
	return o
}

// SetOnConnectHandler sets the function to be called when the client is connected. Both
// at initial connection time and upon automatic reconnect.
func (o *ClientOptions) SetOnConnectHandler(onConn OnConnectHandler) *ClientOptions {
	o.OnConnect = onConn
	return o
}

// SetConnectionLostHandler will set the OnConnectionLost callback to be executed
// in the case where the client unexpectedly loses connection with the MQTT broker.
func (o *ClientOptions) SetConnectionLostHandler(onLost ConnectionLostHandler) *ClientOptions {
	o.OnConnectionLost = onLost
	return o
}

// SetWriteTimeout puts a limit on how long a mqtt publish should block until it unblocks with a
// timeout error. A duration of 0 never times out. Default 30 seconds
func (o *ClientOptions) SetWriteTimeout(t time.Duration) *ClientOptions {
	o.WriteTimeout = t
	return o
}

// SetConnectTimeout limits how long the client will wait when trying to open a connection
// to an MQTT server before timeing out and erroring the attempt. A duration of 0 never times out.
// Default 30 seconds. Currently only operational on TCP/TLS connections.
func (o *ClientOptions) SetConnectTimeout(t time.Duration) *ClientOptions {
	o.ConnectTimeout = t
	return o
}

// SetMaxReconnectInterval sets the maximum time that will be waited between reconnection attempts
// when connection is lost
func (o *ClientOptions) SetMaxReconnectInterval(t time.Duration) *ClientOptions {
	o.MaxReconnectInterval = t
	return o
}

// SetAutoReconnect sets whether the automatic reconnection logic should be used
// when the connection is lost, even if disabled the ConnectionLostHandler is still
// called
func (o *ClientOptions) SetAutoReconnect(a bool) *ClientOptions {
	o.AutoReconnect = a
	return o
}

// SetMessageChannelDepth sets the size of the internal queue that holds messages while the
// client is temporairily offline, allowing the application to publish when the client is
// reconnecting. This setting is only valid if AutoReconnect is set to true, it is otherwise
// ignored.
func (o *ClientOptions) SetMessageChannelDepth(s uint) *ClientOptions {
	o.MessageChannelDepth = s
	return o
}

// SetHTTPHeaders sets the additional HTTP headers that will be sent in the WebSocket
// opening handshake.
func (o *ClientOptions) SetHTTPHeaders(h http.Header) *ClientOptions {
	o.HTTPHeaders = h
	return o
}
