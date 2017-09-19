package authentication

import (
	"crypto/md5"
	"fmt"
	"strings"

	"golang.org/x/crypto/ssh"
)

// formatPublicKeyFingerprint produces the MD5 fingerprint of the given SSH
// public key. If display is true, the fingerprint is formatted with colons
// between each byte, as per the output of OpenSSL.
func formatPublicKeyFingerprint(key ssh.PublicKey, display bool) string {
	publicKeyFingerprint := md5.New()
	publicKeyFingerprint.Write(key.Marshal())
	publicKeyFingerprintString := fmt.Sprintf("%x", publicKeyFingerprint.Sum(nil))

	if !display {
		return publicKeyFingerprintString
	}

	formatted := ""
	for i := 0; i < len(publicKeyFingerprintString); i = i + 2 {
		formatted = fmt.Sprintf("%s%s:", formatted, publicKeyFingerprintString[i:i+2])
	}

	return strings.TrimSuffix(formatted, ":")
}
