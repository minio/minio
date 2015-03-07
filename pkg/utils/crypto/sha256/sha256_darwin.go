package sha256

import (
	"io"

	"crypto/sha256"
)

// Sum256 - single caller sha256 helper
func Sum256(data []byte) []byte {
	d := sha256.New()
	d.Write(data)
	return d.Sum(nil)
}

// Sum - io.Reader based streaming sha256 helper
func Sum(reader io.Reader) ([]byte, error) {
	d := sha256.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		d.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return d.Sum(nil), nil
}
