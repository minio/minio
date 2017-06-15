package packets

import (
	"fmt"
	"io"
)

//PingrespPacket is an internal representation of the fields of the
//Pingresp MQTT packet
type PingrespPacket struct {
	FixedHeader
}

func (pr *PingrespPacket) String() string {
	str := fmt.Sprintf("%s", pr.FixedHeader)
	return str
}

func (pr *PingrespPacket) Write(w io.Writer) error {
	packet := pr.FixedHeader.pack()
	_, err := packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (pr *PingrespPacket) Unpack(b io.Reader) error {
	return nil
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (pr *PingrespPacket) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
