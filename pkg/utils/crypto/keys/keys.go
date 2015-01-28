package keys

import (
	"crypto/rand"
)

var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var alphaNumericTableFull = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

func GetRandomAlphaNumeric(size int) ([]byte, error) {
	alpha := make([]byte, size)
	_, err := rand.Read(alpha)
	if err != nil {
		return nil, err
	}

	for i := 0; i < size; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha, nil
}

func GetRandomAlphaNumericFull(size int) ([]byte, error) {
	alphaFull := make([]byte, size)
	_, err := rand.Read(alphaFull)
	if err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		alphaFull[i] = alphaNumericTableFull[alphaFull[i]%byte(len(alphaNumericTableFull))]
	}
	return alphaFull, nil
}

func GetRandomBase64(size int) ([]byte, error) {
	rb := make([]byte, size)
	n, err := rand.Read(rb)
	if err != nil {
		return nil, err
	}
	dest := make([]byte, n)
	base64.URLEncoding.EncodeTo(dest, rb)
	return dest, nil
}

func ValidateAccessKey(key []byte) bool {
	for _, char := range key {
		if isalnum(char) {
			continue
		}
		switch char {
		case '-':
		case '.':
		case '_':
		case '~':
			continue
		default:
			return false
		}
	}
	return true
}
