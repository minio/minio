package authentication

const authorizationHeaderFormat = `Signature keyId="%s",algorithm="%s",headers="%s",signature="%s"`

type Signer interface {
	DefaultAlgorithm() string
	KeyFingerprint() string
	Sign(dateHeader string) (string, error)
	SignRaw(toSign string) (string, string, error)
}
