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
	"bytes"
	"strings"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/minio/minio-xl/pkg/probe"
	"golang.org/x/crypto/bcrypt"
)

// JWT - jwt auth backend
type JWT struct {
	accessKeyID     []byte
	secretAccessKey []byte
}

// Default - each token expires in 10hrs.
const (
	tokenExpires time.Duration = 10
)

// InitJWT - initialize.
func InitJWT() *JWT {
	jwt := &JWT{}
	// Load credentials.
	config, err := loadConfigV2()
	fatalIf(err.Trace("JWT"), "Unable to load configuration file.", nil)

	// Save access, secret keys.
	jwt.accessKeyID = []byte(config.Credentials.AccessKeyID)
	jwt.secretAccessKey = []byte(config.Credentials.SecretAccessKey)
	return jwt
}

// GenerateToken - generates a new Json Web Token based on the incoming user id.
func (jwt *JWT) GenerateToken(userName string) (string, *probe.Error) {
	token := jwtgo.New(jwtgo.SigningMethodHS512)
	// Token expires in 10hrs.
	token.Claims["exp"] = time.Now().Add(time.Hour * tokenExpires).Unix()
	token.Claims["iat"] = time.Now().Unix()
	token.Claims["sub"] = userName
	tokenString, e := token.SignedString(jwt.secretAccessKey)
	if e != nil {
		return "", probe.NewError(e)
	}
	return tokenString, nil
}

// Authenticate - authenticates incoming username and password.
func (jwt *JWT) Authenticate(userName, password string) bool {
	userName = strings.TrimSpace(userName)
	password = strings.TrimSpace(password)
	if !bytes.Equal([]byte(userName), jwt.accessKeyID) {
		return false
	}
	hashedPassword, _ := bcrypt.GenerateFromPassword(jwt.secretAccessKey, bcrypt.DefaultCost)
	return bcrypt.CompareHashAndPassword(hashedPassword, []byte(password)) == nil
}
