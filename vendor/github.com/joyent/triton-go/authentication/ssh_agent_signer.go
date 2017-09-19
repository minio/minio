package authentication

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/hashicorp/errwrap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type SSHAgentSigner struct {
	formattedKeyFingerprint string
	keyFingerprint          string
	algorithm               string
	accountName             string
	keyIdentifier           string

	agent agent.Agent
	key   ssh.PublicKey
}

func NewSSHAgentSigner(keyFingerprint, accountName string) (*SSHAgentSigner, error) {
	sshAgentAddress := os.Getenv("SSH_AUTH_SOCK")
	if sshAgentAddress == "" {
		return nil, errors.New("SSH_AUTH_SOCK is not set")
	}

	conn, err := net.Dial("unix", sshAgentAddress)
	if err != nil {
		return nil, errwrap.Wrapf("Error dialing SSH agent: {{err}}", err)
	}

	ag := agent.NewClient(conn)

	keys, err := ag.List()
	if err != nil {
		return nil, errwrap.Wrapf("Error listing keys in SSH Agent: %s", err)
	}

	keyFingerprintStripped := strings.TrimPrefix(keyFingerprint, "MD5:")
	keyFingerprintStripped = strings.TrimPrefix(keyFingerprintStripped, "SHA256:")
	keyFingerprintStripped = strings.Replace(keyFingerprintStripped, ":", "", -1)

	var matchingKey ssh.PublicKey
	for _, key := range keys {
		keyMD5 := md5.New()
		keyMD5.Write(key.Marshal())
		finalizedMD5 := fmt.Sprintf("%x", keyMD5.Sum(nil))

		keySHA256 := sha256.New()
		keySHA256.Write(key.Marshal())
		finalizedSHA256 := base64.RawStdEncoding.EncodeToString(keySHA256.Sum(nil))

		if keyFingerprintStripped == finalizedMD5 || keyFingerprintStripped == finalizedSHA256 {
			matchingKey = key
		}
	}

	if matchingKey == nil {
		return nil, fmt.Errorf("No key in the SSH Agent matches fingerprint: %s", keyFingerprint)
	}

	formattedKeyFingerprint := formatPublicKeyFingerprint(matchingKey, true)

	signer := &SSHAgentSigner{
		formattedKeyFingerprint: formattedKeyFingerprint,
		keyFingerprint:          keyFingerprint,
		accountName:             accountName,
		agent:                   ag,
		key:                     matchingKey,
		keyIdentifier:           fmt.Sprintf("/%s/keys/%s", accountName, formattedKeyFingerprint),
	}

	_, algorithm, err := signer.SignRaw("HelloWorld")
	if err != nil {
		return nil, fmt.Errorf("Cannot sign using ssh agent: %s", err)
	}
	signer.algorithm = algorithm

	return signer, nil
}

func (s *SSHAgentSigner) Sign(dateHeader string) (string, error) {
	const headerName = "date"

	signature, err := s.agent.Sign(s.key, []byte(fmt.Sprintf("%s: %s", headerName, dateHeader)))
	if err != nil {
		return "", errwrap.Wrapf("Error signing date header: {{err}}", err)
	}

	keyFormat, err := keyFormatToKeyType(signature.Format)
	if err != nil {
		return "", errwrap.Wrapf("Error reading signature: {{err}}", err)
	}

	var authSignature httpAuthSignature
	switch keyFormat {
	case "rsa":
		authSignature, err = newRSASignature(signature.Blob)
		if err != nil {
			return "", errwrap.Wrapf("Error reading signature: {{err}}", err)
		}
	case "ecdsa":
		authSignature, err = newECDSASignature(signature.Blob)
		if err != nil {
			return "", errwrap.Wrapf("Error reading signature: {{err}}", err)
		}
	default:
		return "", fmt.Errorf("Unsupported algorithm from SSH agent: %s", signature.Format)
	}

	return fmt.Sprintf(authorizationHeaderFormat, s.keyIdentifier,
		authSignature.SignatureType(), headerName, authSignature.String()), nil
}

func (s *SSHAgentSigner) SignRaw(toSign string) (string, string, error) {
	signature, err := s.agent.Sign(s.key, []byte(toSign))
	if err != nil {
		return "", "", errwrap.Wrapf("Error signing string: {{err}}", err)
	}

	keyFormat, err := keyFormatToKeyType(signature.Format)
	if err != nil {
		return "", "", errwrap.Wrapf("Error reading signature: {{err}}", err)
	}

	var authSignature httpAuthSignature
	switch keyFormat {
	case "rsa":
		authSignature, err = newRSASignature(signature.Blob)
		if err != nil {
			return "", "", errwrap.Wrapf("Error reading signature: {{err}}", err)
		}
	case "ecdsa":
		authSignature, err = newECDSASignature(signature.Blob)
		if err != nil {
			return "", "", errwrap.Wrapf("Error reading signature: {{err}}", err)
		}
	default:
		return "", "", fmt.Errorf("Unsupported algorithm from SSH agent: %s", signature.Format)
	}

	return authSignature.String(), authSignature.SignatureType(), nil
}

func (s *SSHAgentSigner) KeyFingerprint() string {
	return s.formattedKeyFingerprint
}

func (s *SSHAgentSigner) DefaultAlgorithm() string {
	return s.algorithm
}
