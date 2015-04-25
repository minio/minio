package main

import (
	"fmt"
	"io"
	"os"

	"crypto/md5"
)

// mustHashBinarySelf computes MD5SUM of a binary file on disk
func hashBinary(progName string) (string, error) {
	h := md5.New()

	file, err := os.Open(progName) // For read access.
	if err != nil {
		return "", err
	}

	io.Copy(h, file)
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// mustHashBinarySelf computes MD5SUM of its binary file on disk
func mustHashBinarySelf() string {
	hash, _ := hashBinary(os.Args[0])
	return hash
}
