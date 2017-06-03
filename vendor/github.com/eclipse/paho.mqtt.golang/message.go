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
	"github.com/eclipse/paho.mqtt.golang/packets"
)

// Message defines the externals that a message implementation must support
// these are received messages that are passed to the callbacks, not internal
// messages
type Message interface {
	Duplicate() bool
	Qos() byte
	Retained() bool
	Topic() string
	MessageID() uint16
	Payload() []byte
}

type message struct {
	duplicate bool
	qos       byte
	retained  bool
	topic     string
	messageID uint16
	payload   []byte
}

func (m *message) Duplicate() bool {
	return m.duplicate
}

func (m *message) Qos() byte {
	return m.qos
}

func (m *message) Retained() bool {
	return m.retained
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) MessageID() uint16 {
	return m.messageID
}

func (m *message) Payload() []byte {
	return m.payload
}

func messageFromPublish(p *packets.PublishPacket) Message {
	return &message{
		duplicate: p.Dup,
		qos:       p.Qos,
		retained:  p.Retain,
		topic:     p.TopicName,
		messageID: p.MessageID,
		payload:   p.Payload,
	}
}

func newConnectMsgFromOptions(options *ClientOptions) *packets.ConnectPacket {
	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)

	m.CleanSession = options.CleanSession
	m.WillFlag = options.WillEnabled
	m.WillRetain = options.WillRetained
	m.ClientIdentifier = options.ClientID

	if options.WillEnabled {
		m.WillQos = options.WillQos
		m.WillTopic = options.WillTopic
		m.WillMessage = options.WillPayload
	}

	if options.Username != "" {
		m.UsernameFlag = true
		m.Username = options.Username
		//mustn't have password without user as well
		if options.Password != "" {
			m.PasswordFlag = true
			m.Password = []byte(options.Password)
		}
	}

	m.Keepalive = uint16(options.KeepAlive.Seconds())

	return m
}
