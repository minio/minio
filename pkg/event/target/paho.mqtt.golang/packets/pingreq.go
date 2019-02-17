package packets

import (
	"fmt"
	"io"
)

//PingreqPacket is an internal representation of the fields of the
//Pingreq MQTT packet
type PingreqPacket struct {
	FixedHeader
}

func (pr *PingreqPacket) String() string {
	str := fmt.Sprintf("%s", pr.FixedHeader)
	return str
}

func (pr *PingreqPacket) Write(w io.Writer) error {
	packet := pr.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pr *PingreqPacket) Unpack(b io.Reader) error {
	return nil
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (pr *PingreqPacket) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
