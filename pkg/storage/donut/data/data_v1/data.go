package data_v1

import "errors"

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
	K         int
	M         int
	Technique EncoderTechnique
}

func Write() error {
	return errors.New("Not Implemented")
}
