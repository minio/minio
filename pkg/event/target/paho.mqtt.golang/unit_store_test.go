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
	"io/ioutil"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func Test_fullpath(t *testing.T) {
	p := fullpath("/tmp/store", "o.44324")
	e := "/tmp/store/o.44324.msg"
	if p != e {
		t.Fatalf("full path expected %s, got %s", e, p)
	}
}

func Test_exists(t *testing.T) {
	b := exists("/")
	if !b {
		t.Errorf("/proc/cpuinfo was not found")
	}
}

func Test_exists_no(t *testing.T) {
	b := exists("/this/path/is/not/real/i/hope")
	if b {
		t.Errorf("you have some strange files")
	}
}

func isemptydir(dir string) bool {
	if !exists(dir) {
		panic(fmt.Errorf("Directory %s does not exist", dir))
	}
	files, err := ioutil.ReadDir(dir)
	chkerr(err)
	return len(files) == 0
}

func Test_mIDFromKey(t *testing.T) {
	key := "i.123"
	exp := uint16(123)
	res := mIDFromKey(key)
	if exp != res {
		t.Fatalf("mIDFromKey failed")
	}
}

func Test_inboundKeyFromMID(t *testing.T) {
	id := uint16(9876)
	exp := "i.9876"
	res := inboundKeyFromMID(id)
	if exp != res {
		t.Fatalf("inboundKeyFromMID failed")
	}
}

func Test_outboundKeyFromMID(t *testing.T) {
	id := uint16(7654)
	exp := "o.7654"
	res := outboundKeyFromMID(id)
	if exp != res {
		t.Fatalf("outboundKeyFromMID failed")
	}
}

/************************
 **** persistOutbound ****
 ************************/

func Test_persistOutbound_connect(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	m.Qos = 0
	m.Username = "user"
	m.Password = []byte("pass")
	m.ClientIdentifier = "cid"
	//m := newConnectMsg(false, false, QOS_ZERO, false, "", nil, "cid", "user", "pass", 10)
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_publish_0(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.Qos = 0
	m.TopicName = "/popub0"
	m.Payload = []byte{0xBB, 0x00}
	m.MessageID = 40
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_publish_1(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.Qos = 1
	m.TopicName = "/popub1"
	m.Payload = []byte{0xBB, 0x00}
	m.MessageID = 41
	persistOutbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 41 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_publish_2(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.Qos = 2
	m.TopicName = "/popub2"
	m.Payload = []byte{0xBB, 0x00}
	m.MessageID = 42
	persistOutbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 42 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_puback(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 1 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_pubrec(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_pubrel(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	m.MessageID = 43

	persistOutbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 43 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_pubcomp(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 1 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_subscribe(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	m.Topics = []string{"/posub"}
	m.Qoss = []byte{1}
	m.MessageID = 44
	persistOutbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 44 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_unsubscribe(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	m.Topics = []string{"/posub"}
	m.MessageID = 45
	persistOutbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 45 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_pingreq(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Pingreq)
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

func Test_persistOutbound_disconnect(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Disconnect)
	persistOutbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistOutbound put message it should not have")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistOutbound get message it should not have")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistOutbound del message it should not have")
	}
}

/************************
 **** persistInbound ****
 ************************/

func Test_persistInbound_connack(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Connack)
	persistInbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_publish_0(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.Qos = 0
	m.TopicName = "/pipub0"
	m.Payload = []byte{0xCC, 0x01}
	m.MessageID = 50
	persistInbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_publish_1(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.Qos = 1
	m.TopicName = "/pipub1"
	m.Payload = []byte{0xCC, 0x02}
	m.MessageID = 51
	persistInbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 51 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_publish_2(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.Qos = 2
	m.TopicName = "/pipub2"
	m.Payload = []byte{0xCC, 0x03}
	m.MessageID = 52
	persistInbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 52 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_puback(t *testing.T) {
	ts := &TestStore{}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = 1
	pub.TopicName = "/pub1"
	pub.Payload = []byte{0xCC, 0x04}
	pub.MessageID = 53
	publishKey := inboundKeyFromMID(pub.MessageID)
	ts.Put(publishKey, pub)

	m := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	m.MessageID = 53

	persistInbound(ts, m) // "deletes" packets.Publish from store

	if len(ts.mput) != 1 { // not actually deleted in TestStore
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 1 || ts.mdel[0] != 53 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_pubrec(t *testing.T) {
	ts := &TestStore{}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = 2
	pub.TopicName = "/pub2"
	pub.Payload = []byte{0xCC, 0x05}
	pub.MessageID = 54
	publishKey := inboundKeyFromMID(pub.MessageID)
	ts.Put(publishKey, pub)

	m := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	m.MessageID = 54

	persistInbound(ts, m)

	if len(ts.mput) != 1 || ts.mput[0] != 54 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_pubrel(t *testing.T) {
	ts := &TestStore{}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = 2
	pub.TopicName = "/pub2"
	pub.Payload = []byte{0xCC, 0x06}
	pub.MessageID = 55
	publishKey := inboundKeyFromMID(pub.MessageID)
	ts.Put(publishKey, pub)

	m := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	m.MessageID = 55

	persistInbound(ts, m) // will overwrite publish

	if len(ts.mput) != 2 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_pubcomp(t *testing.T) {
	ts := &TestStore{}

	m := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	m.MessageID = 56

	persistInbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 1 || ts.mdel[0] != 56 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_suback(t *testing.T) {
	ts := &TestStore{}

	m := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	m.MessageID = 57

	persistInbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 1 || ts.mdel[0] != 57 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_unsuback(t *testing.T) {
	ts := &TestStore{}

	m := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	m.MessageID = 58

	persistInbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 1 || ts.mdel[0] != 58 {
		t.Fatalf("persistInbound in bad state")
	}
}

func Test_persistInbound_pingresp(t *testing.T) {
	ts := &TestStore{}
	m := packets.NewControlPacket(packets.Pingresp)

	persistInbound(ts, m)

	if len(ts.mput) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mget) != 0 {
		t.Fatalf("persistInbound in bad state")
	}

	if len(ts.mdel) != 0 {
		t.Fatalf("persistInbound in bad state")
	}
}
