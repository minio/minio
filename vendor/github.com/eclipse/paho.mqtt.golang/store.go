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
	"fmt"
	"strconv"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	inboundPrefix  = "i."
	outboundPrefix = "o."
)

// Store is an interface which can be used to provide implementations
// for message persistence.
// Because we may have to store distinct messages with the same
// message ID, we need a unique key for each message. This is
// possible by prepending "i." or "o." to each message id
type Store interface {
	Open()
	Put(key string, message packets.ControlPacket)
	Get(key string) packets.ControlPacket
	All() []string
	Del(key string)
	Close()
	Reset()
}

// A key MUST have the form "X.[messageid]"
// where X is 'i' or 'o'
func mIDFromKey(key string) uint16 {
	s := key[2:]
	i, err := strconv.Atoi(s)
	chkerr(err)
	return uint16(i)
}

// Return true if key prefix is outbound
func isKeyOutbound(key string) bool {
	return key[:2] == outboundPrefix
}

// Return true if key prefix is inbound
func isKeyInbound(key string) bool {
	return key[:2] == inboundPrefix
}

// Return a string of the form "i.[id]"
func inboundKeyFromMID(id uint16) string {
	return fmt.Sprintf("%s%d", inboundPrefix, id)
}

// Return a string of the form "o.[id]"
func outboundKeyFromMID(id uint16) string {
	return fmt.Sprintf("%s%d", outboundPrefix, id)
}

// govern which outgoing messages are persisted
func persistOutbound(s Store, m packets.ControlPacket) {
	switch m.Details().Qos {
	case 0:
		switch m.(type) {
		case *packets.PubackPacket, *packets.PubcompPacket:
			// Sending puback. delete matching publish
			// from ibound
			s.Del(inboundKeyFromMID(m.Details().MessageID))
		}
	case 1:
		switch m.(type) {
		case *packets.PublishPacket, *packets.PubrelPacket, *packets.SubscribePacket, *packets.UnsubscribePacket:
			// Sending publish. store in obound
			// until puback received
			s.Put(outboundKeyFromMID(m.Details().MessageID), m)
		default:
			ERROR.Println(STR, "Asked to persist an invalid message type")
		}
	case 2:
		switch m.(type) {
		case *packets.PublishPacket:
			// Sending publish. store in obound
			// until pubrel received
			s.Put(outboundKeyFromMID(m.Details().MessageID), m)
		default:
			ERROR.Println(STR, "Asked to persist an invalid message type")
		}
	}
}

// govern which incoming messages are persisted
func persistInbound(s Store, m packets.ControlPacket) {
	switch m.Details().Qos {
	case 0:
		switch m.(type) {
		case *packets.PubackPacket, *packets.SubackPacket, *packets.UnsubackPacket, *packets.PubcompPacket:
			// Received a puback. delete matching publish
			// from obound
			s.Del(outboundKeyFromMID(m.Details().MessageID))
		case *packets.PublishPacket, *packets.PubrecPacket, *packets.PingrespPacket, *packets.ConnackPacket:
		default:
			ERROR.Println(STR, "Asked to persist an invalid messages type")
		}
	case 1:
		switch m.(type) {
		case *packets.PublishPacket, *packets.PubrelPacket:
			// Received a publish. store it in ibound
			// until puback sent
			s.Put(inboundKeyFromMID(m.Details().MessageID), m)
		default:
			ERROR.Println(STR, "Asked to persist an invalid messages type")
		}
	case 2:
		switch m.(type) {
		case *packets.PublishPacket:
			// Received a publish. store it in ibound
			// until pubrel received
			s.Put(inboundKeyFromMID(m.Details().MessageID), m)
		default:
			ERROR.Println(STR, "Asked to persist an invalid messages type")
		}
	}
}
