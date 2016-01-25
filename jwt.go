package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/bcrypt"
)

// JWT - jwt auth backend
type JWT struct {
	privateKey      *rsa.PrivateKey
	PublicKey       *rsa.PublicKey
	accessKeyID     string
	secretAccessKey string
}

const (
	jwtExpirationDelta = 10
	expireOffset       = 3600
)

// InitJWT - init.
func InitJWT() *JWT {
	jwt := &JWT{
		privateKey: getPrivateKey(),
		PublicKey:  getPublicKey(),
	}
	config, err := loadConfigV2()
	fatalIf(err.Trace("JWT"), "Unable to load configuration file.", nil)

	// Save access, secret keys.
	jwt.accessKeyID = config.Credentials.AccessKeyID
	jwt.secretAccessKey = config.Credentials.SecretAccessKey
	return jwt
}

// GenerateToken -
func (b *JWT) GenerateToken(userName string) (string, error) {
	token := jwt.New(jwt.SigningMethodRS512)
	token.Claims["exp"] = time.Now().Add(time.Hour * time.Duration(jwtExpirationDelta)).Unix()
	token.Claims["iat"] = time.Now().Unix()
	token.Claims["sub"] = userName
	tokenString, err := token.SignedString(b.privateKey)
	if err != nil {
		return "", err
	}
	return tokenString, nil
}

// Authenticate -
func (b *JWT) Authenticate(args *LoginArgs) bool {
	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte(b.secretAccessKey), 10)
	if args.Username == b.accessKeyID {
		return bcrypt.CompareHashAndPassword(hashedPassword, []byte(args.Password)) == nil
	}
	return false
}

//
func (b *JWT) getTokenRemainingValidity(timestamp interface{}) int {
	if validity, ok := timestamp.(float64); ok {
		tm := time.Unix(int64(validity), 0)
		remainer := tm.Sub(time.Now())
		if remainer > 0 {
			return int(remainer.Seconds() + expireOffset)
		}
	}
	return expireOffset
}

// Logout - logout is not implemented yet.
func (b *JWT) Logout(tokenString string) error {
	return nil
}

func getPrivateKey() *rsa.PrivateKey {
	pemBytes, err := ioutil.ReadFile(mustGetPrivateKeyPath())
	if err != nil {
		panic(err)
	}
	data, _ := pem.Decode([]byte(pemBytes))
	privateKeyImported, err := x509.ParsePKCS1PrivateKey(data.Bytes)
	if err != nil {
		panic(err)
	}
	return privateKeyImported
}

func getPublicKey() *rsa.PublicKey {
	pemBytes, err := ioutil.ReadFile(mustGetPublicKeyPath())
	if err != nil {
		panic(err)
	}
	data, _ := pem.Decode([]byte(pemBytes))
	publicKeyImported, err := x509.ParsePKIXPublicKey(data.Bytes)
	if err != nil {
		panic(err)
	}

	rsaPub, ok := publicKeyImported.(*rsa.PublicKey)
	if !ok {
		panic(err)
	}

	return rsaPub
}
