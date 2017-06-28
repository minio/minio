package packets

import (
	"fmt"
	"io"
)

//UnsubackPacket is an internal representation of the fields of the
//Unsuback MQTT packet
type UnsubackPacket struct {
	FixedHeader
	MessageID uint16
}

func (ua *UnsubackPacket) String() string {
	str := fmt.Sprintf("%s", ua.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageID: %d", ua.MessageID)
	return str
}

func (ua *UnsubackPacket) Write(w io.Writer) error {
	var err error
	ua.FixedHeader.RemainingLength = 2
	packet := ua.FixedHeader.pack()
	packet.Write(encodeUint16(ua.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (ua *UnsubackPacket) Unpack(b io.Reader) error {
	ua.MessageID = decodeUint16(b)

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (ua *UnsubackPacket) Details() Details {
	return Details{Qos: 0, MessageID: ua.MessageID}
}
