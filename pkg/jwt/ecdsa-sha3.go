package jwt

import (
	"crypto"
	"github.com/dgrijalva/jwt-go"
	// Needed for SHA3 to work
	_ "golang.org/x/crypto/sha3"
)

// Specific instances for EC256 and company
var (
	SigningMethodES3256 *jwt.SigningMethodECDSA
	SigningMethodES3384 *jwt.SigningMethodECDSA
	SigningMethodES3512 *jwt.SigningMethodECDSA
)

func init() {
	// ES256
	SigningMethodES3256 = &jwt.SigningMethodECDSA{Name: "ES3256", Hash: crypto.SHA3_256, KeySize: 32, CurveBits: 256}
	jwt.RegisterSigningMethod(SigningMethodES3256.Alg(), func() jwt.SigningMethod {
		return SigningMethodES3256
	})

	// ES384
	SigningMethodES3384 = &jwt.SigningMethodECDSA{Name: "ES3384", Hash: crypto.SHA3_384, KeySize: 48, CurveBits: 384}
	jwt.RegisterSigningMethod(SigningMethodES3384.Alg(), func() jwt.SigningMethod {
		return SigningMethodES3384
	})

	// ES512
	SigningMethodES3512 = &jwt.SigningMethodECDSA{Name: "ES3512", Hash: crypto.SHA3_512, KeySize: 66, CurveBits: 521}
	jwt.RegisterSigningMethod(SigningMethodES3512.Alg(), func() jwt.SigningMethod {
		return SigningMethodES3512
	})
}
