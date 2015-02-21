// +build ignore

package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	sha1intel "github.com/minio-io/minio/pkg/utils/crypto/sha1"
)

func Sum(reader io.Reader) ([]byte, error) {
	k := sha1.New()
	var err error
	for err == nil {
		length := 0
		byteBuffer := make([]byte, 1024*1024)
		length, err = reader.Read(byteBuffer)
		byteBuffer = byteBuffer[0:length]
		k.Write(byteBuffer)
	}
	if err != io.EOF {
		return nil, err
	}
	return k.Sum(nil), nil
}

func main() {
	fmt.Println("-- start")

	file1, _ := os.Open("filename1")
	defer file1.Close()
	stark := time.Now()
	sum, _ := Sum(file1)
	endk := time.Since(stark)

	file2, _ := os.Open("filename2")
	defer file2.Close()
	starth := time.Now()
	sumAVX2, _ := sha1intel.Sum(file2)
	endh := time.Since(starth)

	fmt.Println("std(", endk, ")", "avx2(", endh, ")")
	fmt.Println(hex.EncodeToString(sum), hex.EncodeToString(sumAVX2))
}
