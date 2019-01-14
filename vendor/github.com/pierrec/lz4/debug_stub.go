// +build !lz4debug

package lz4

const debugFlag = false

func debug(args ...interface{}) {}
