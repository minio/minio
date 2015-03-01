package data_v1

import "errors"

type DataHeader struct {
	Key           string
	Part          int
	ContentType   string
	Length        uint64
	Md5sum        []byte
	EncoderParams EncoderParams
}

type EncoderParams struct {
	Length    int
	K         int
	M         int
	Technique int
}

func Write() error {
	return errors.New("Not Implemented")
}
