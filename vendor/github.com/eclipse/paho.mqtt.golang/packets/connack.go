package packets

import (
	"bytes"
	"fmt"
	"io"
)

//ConnackPacket is an internal representation of the fields of the
//Connack MQTT packet
type ConnackPacket struct {
	FixedHeader
	SessionPresent bool
	ReturnCode     byte
}

func (ca *ConnackPacket) String() string {
	str := fmt.Sprintf("%s", ca.FixedHeader)
	str += " "
	str += fmt.Sprintf("sessionpresent: %t returncode: %d", ca.SessionPresent, ca.ReturnCode)
	return str
}

func (ca *ConnackPacket) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error

	body.WriteByte(boolToByte(ca.SessionPresent))
	body.WriteByte(ca.ReturnCode)
	ca.FixedHeader.RemainingLength = 2
	packet := ca.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

//Unpack decodes the details of a ControlPacket after the fixed
//header has been read
func (ca *ConnackPacket) Unpack(b io.Reader) error {
	ca.SessionPresent = 1&decodeByte(b) > 0
	ca.ReturnCode = decodeByte(b)

	return nil
}

//Details returns a Details struct containing the Qos and
//MessageID of this ControlPacket
func (ca *ConnackPacket) Details() Details {
	return Details{Qos: 0, MessageID: 0}
}
