package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"crypto/md5"
)

// hashBinary computes MD5SUM of a binary file on disk
func hashBinary(progName string) (string, error) {
	path, err := exec.LookPath(progName)
	if err != nil {
		return "", err
	}

	m := md5.New()

	file, err := os.Open(path) // For read access.
	if err != nil {
		return "", err
	}

	io.Copy(m, file)
	return fmt.Sprintf("%x", m.Sum(nil)), nil
}

// mustHashBinarySelf masks any error returned by hashBinary
func mustHashBinarySelf() string {
	hash, _ := hashBinary(os.Args[0])
	return hash
}
