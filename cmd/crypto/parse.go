package crypto

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ParseKMSMasterKey parses the value of the environment variable
// `EnvKMSMasterKey` and returns a key-ID and a master-key KMS on success.
func ParseKMSMasterKey(envArg string) (string, KMS, error) {
	values := strings.SplitN(envArg, ":", 2)
	if len(values) != 2 {
		return "", nil, fmt.Errorf("Invalid KMS master key: %s does not contain a ':'", envArg)
	}
	var (
		keyID  = values[0]
		hexKey = values[1]
	)
	if len(hexKey) != 64 { // 2 hex bytes = 1 byte
		return "", nil, fmt.Errorf("Invalid KMS master key: %s not a 32 bytes long HEX value", hexKey)
	}
	var masterKey [32]byte
	if _, err := hex.Decode(masterKey[:], []byte(hexKey)); err != nil {
		return "", nil, fmt.Errorf("Invalid KMS master key: %s not a 32 bytes long HEX value", hexKey)
	}
	return keyID, NewKMS(masterKey), nil
}
