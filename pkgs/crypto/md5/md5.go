package md5

import (
	"crypto/md5"
	"io"
)

func Sum(reader io.Reader) ([]byte, error) {
	hash := md5.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		hash.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return hash.Sum(nil), nil
}
