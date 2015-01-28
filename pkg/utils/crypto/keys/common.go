package keys

const (
	MINIO_ACCESS_ID = 20
	MINIO_SECRET_ID = 40
)

func isalnum(c byte) bool {
	return '0' <= c && c <= '9' || 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z'
}
