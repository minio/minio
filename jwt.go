/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/minio/minio-xl/pkg/probe"
	"golang.org/x/crypto/bcrypt"
)

// JWT - jwt auth backend
type JWT struct {
	// Public value.
	PublicKey *rsa.PublicKey
	// private values.
	privateKey      *rsa.PrivateKey
	accessKeyID     string
	secretAccessKey string
}

const (
	jwtExpirationDelta = 10
)

// InitJWT - initialize.
func InitJWT() *JWT {
	jwt := &JWT{
		privateKey: getPrivateKey(),
	}
	// Validate if public key is of algorithm *rsa.PublicKey.
	var ok bool
	jwt.PublicKey, ok = jwt.privateKey.Public().(*rsa.PublicKey)
	if !ok {
		fatalIf(probe.NewError(errors.New("")), "Unsupported type of public key algorithm found.", nil)
	}
	// Load credentials configuration.
	config, err := loadConfigV2()
	fatalIf(err.Trace("JWT"), "Unable to load configuration file.", nil)

	// Save access, secret keys.
	jwt.accessKeyID = config.Credentials.AccessKeyID
	jwt.secretAccessKey = config.Credentials.SecretAccessKey
	return jwt
}

// GenerateToken - generates a new Json Web Token based on the incoming user id.
func (b *JWT) GenerateToken(userName string) (string, error) {
	token := jwtgo.New(jwtgo.SigningMethodRS512)
	// Token expires in 10hrs.
	token.Claims["exp"] = time.Now().Add(time.Hour * time.Duration(jwtExpirationDelta)).Unix()
	token.Claims["iat"] = time.Now().Unix()
	token.Claims["sub"] = userName
	tokenString, err := token.SignedString(b.privateKey)
	if err != nil {
		return "", err
	}
	return tokenString, nil
}

// Authenticate - authenticates the username and password.
func (b *JWT) Authenticate(username, password string) bool {
	hashedPassword, _ := bcrypt.GenerateFromPassword([]byte(b.secretAccessKey), 10)
	if username == b.accessKeyID {
		return bcrypt.CompareHashAndPassword(hashedPassword, []byte(password)) == nil
	}
	return false
}

// getPrivateKey - get the generated private key.
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
