package authentication

// TestSigner represents an authentication key signer which we can use for
// testing purposes only. This will largely be a stub to send through client
// unit tests.
type TestSigner struct{}

// NewTestSigner constructs a new instance of test signer
func NewTestSigner() (Signer, error) {
	return &TestSigner{}, nil
}

func (s *TestSigner) DefaultAlgorithm() string {
	return ""
}

func (s *TestSigner) KeyFingerprint() string {
	return ""
}

func (s *TestSigner) Sign(dateHeader string) (string, error) {
	return "", nil
}

func (s *TestSigner) SignRaw(toSign string) (string, string, error) {
	return "", "", nil
}
