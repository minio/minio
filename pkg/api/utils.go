package api

import (
	"encoding/base64"
	"strings"
)

func isValidMD5(md5 string) bool {
	if md5 == "" {
		return true
	}
	_, err := base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
	if err != nil {
		return false
	}
	return true
}
