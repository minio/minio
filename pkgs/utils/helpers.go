package utils

import "io/ioutil"

func MakeTempTestDir() (string, error) {
	return ioutil.TempDir("/tmp", "minio-test-")
}
