package authentication

import (
	"regexp"
	"fmt"
)

type httpAuthSignature interface {
	SignatureType() string
	String() string
}

func keyFormatToKeyType(keyFormat string) (string, error) {
	if keyFormat == "ssh-rsa" {
		return "rsa", nil
	}

	if keyFormat == "ssh-ed25519" {
		return "ed25519", nil
	}

	if regexp.MustCompile("^ecdsa-sha2-*").Match([]byte(keyFormat)) {
		return "ecdsa", nil
	}

	return "", fmt.Errorf("Unknown key format: %s", keyFormat)
}
