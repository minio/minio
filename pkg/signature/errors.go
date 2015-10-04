package signature

// MissingDateHeader date header missing
type MissingDateHeader struct{}

func (e MissingDateHeader) Error() string {
	return "Missing date header"
}

// MissingExpiresQuery expires query string missing
type MissingExpiresQuery struct{}

func (e MissingExpiresQuery) Error() string {
	return "Missing expires query string"
}

// ExpiredPresignedRequest request already expired
type ExpiredPresignedRequest struct{}

func (e ExpiredPresignedRequest) Error() string {
	return "Presigned request already expired"
}

// DoesNotMatch invalid signature
type DoesNotMatch struct {
	SignatureSent       string
	SignatureCalculated string
}

func (e DoesNotMatch) Error() string {
	return "The request signature we calculated does not match the signature you provided"
}
