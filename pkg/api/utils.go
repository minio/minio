package api

import (
	"encoding/base64"
	"strings"
)

func isValidMD5(md5 string) bool {
	_, err := base64.StdEncoding.DecodeString(strings.TrimSpace(md5))
	if err != nil {
		return false
	}
	return true
}
