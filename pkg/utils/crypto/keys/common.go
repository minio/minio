package keys

const (
	MINIO_ACCESS_ID = 20
	MINIO_SECRET_ID = 40
)

func isalnum(c byte) bool {
	return '0' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z'
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
