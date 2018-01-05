package triton

import (
	"github.com/joyent/triton-go/authentication"
)

// Universal package used for defining configuration used across all client
// constructors.

// ClientConfig is a placeholder/input struct around the behavior of configuring
// a client constructor through the implementation's runtime environment
// (SDC/MANTA env vars).
type ClientConfig struct {
	TritonURL   string
	MantaURL    string
	AccountName string
	Signers     []authentication.Signer
}
