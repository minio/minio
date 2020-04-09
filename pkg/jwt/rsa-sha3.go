package jwt

import (
	"crypto"
	"github.com/dgrijalva/jwt-go"
	// Needed for SHA3 to work
	_ "golang.org/x/crypto/sha3"
)

// Specific instances for RS256 and company
var (
	SigningMethodRS3256 *jwt.SigningMethodRSA
	SigningMethodRS3384 *jwt.SigningMethodRSA
	SigningMethodRS3512 *jwt.SigningMethodRSA
)

func init() {
	// RS3256
	SigningMethodRS3256 = &jwt.SigningMethodRSA{Name: "RS3256", Hash: crypto.SHA3_256}
	jwt.RegisterSigningMethod(SigningMethodRS3256.Alg(), func() jwt.SigningMethod {
		return SigningMethodRS3256
	})

	// RS3384
	SigningMethodRS3384 = &jwt.SigningMethodRSA{Name: "RS3384", Hash: crypto.SHA3_384}
	jwt.RegisterSigningMethod(SigningMethodRS3384.Alg(), func() jwt.SigningMethod {
		return SigningMethodRS3384
	})

	// RS3512
	SigningMethodRS3512 = &jwt.SigningMethodRSA{Name: "RS3512", Hash: crypto.SHA3_512}
	jwt.RegisterSigningMethod(SigningMethodRS3512.Alg(), func() jwt.SigningMethod {
		return SigningMethodRS3512
	})
}
