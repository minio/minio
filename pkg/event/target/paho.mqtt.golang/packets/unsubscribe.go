package packets

import (
	"bytes"
	"fmt"
	"io"
)

//UnsubscribePacket is an internal representation of the fields of the
//Unsubscribe MQTT packet
type UnsubscribePacket struct {
	FixedHeader
	MessageID uint16
	Topics    []string
}

func (u *UnsubscribePacket) String() string {
	str := fmt.Sprintf("%s", u.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageID: %d", u.MessageID)
	return str
}

func (u *UnsubscribePacket) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(encodeUint16(u.MessageID))
	for _, topic := range u.Topics {
		body.Write(encodeString(topic))
	}
	u.FixedHeader.RemainingLength = body.Len()
	packet := u.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (u *UnsubscribePacket) Unpack(b io.Reader) error {
	u.MessageID = decodeUint16(b)
	var topic string
	for topic = decodeString(b); topic != ""; topic = decodeString(b) {
		u.Topics = append(u.Topics, topic)
	}

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (u *UnsubscribePacket) Details() Details {
	return Details{Qos: 1, MessageID: u.MessageID}
}
