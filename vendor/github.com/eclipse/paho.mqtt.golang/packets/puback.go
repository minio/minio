package packets

import (
	"fmt"
	"io"
)

//PubackPacket is an internal representation of the fields of the
//Puback MQTT packet
type PubackPacket struct {
	FixedHeader
	MessageID uint16
}

func (pa *PubackPacket) String() string {
	str := fmt.Sprintf("%s", pa.FixedHeader)
	str += " "
	str += fmt.Sprintf("MessageID: %d", pa.MessageID)
	return str
}

func (pa *PubackPacket) Write(w io.Writer) error {
	var err error
	pa.FixedHeader.RemainingLength = 2
	packet := pa.FixedHeader.pack()
	packet.Write(encodeUint16(pa.MessageID))
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pa *PubackPacket) Unpack(b io.Reader) error {
	pa.MessageID = decodeUint16(b)

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (pa *PubackPacket) Details() Details {
	return Details{Qos: pa.Qos, MessageID: pa.MessageID}
}
