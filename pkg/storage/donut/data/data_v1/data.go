package data_v1

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
)

type DataHeader struct {
	Key           string
	Part          int
	Metadata      map[string]string
	EncoderParams EncoderParams
}

type EncoderTechnique int

const (
	VANDERMONDE EncoderTechnique = iota
	CAUCHY
)

type EncoderParams struct {
	Length    int
	K         uint8
	M         uint8
	Technique EncoderTechnique
}

func Write(target io.Writer, header DataHeader, data io.Reader) error {
	var headerBuffer bytes.Buffer
	// encode header
	encoder := gob.NewEncoder(&headerBuffer)
	encoder.Encode(header)
	// write length of header
	if err := binary.Write(target, binary.LittleEndian, headerBuffer.Len()); err != nil {
		return err
	}
	// write encoded header
	if _, err := io.Copy(target, &headerBuffer); err != nil {
		return err
	}
	// write data
	if _, err := io.Copy(target, data); err != nil {
		return err
	}
	return nil
}
