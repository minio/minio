package md5

import (
	"crypto/md5"
	"io"
)

func Sum(reader io.Reader) ([]byte, error) {
	hash := md5.New()
	var err error
	var length int
	for err == nil {
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		// While hash.Write() wouldn't mind a Nil byteBuffer
		// It is necessary for us to verify this and break
		if length == 0 {
			break
		}
		byteBuffer = byteBuffer[0:length]
		hash.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return hash.Sum(nil), nil
}
