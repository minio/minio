package main

import (
	"fmt"
	"net/http"

	"github.com/dgrijalva/jwt-go"
)

type authHandler struct {
	handler http.Handler
}

// AuthHandler -
// Verify if authorization header has signature version '2', reject it cleanly.
func AuthHandler(h http.Handler) http.Handler {
	return authHandler{h}
}

// Ignore request if authorization header is not valid.
func (h authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For login attempts please issue a new token.
	if r.Method == "POST" && r.URL.Path == "/login-token" {
		h.handler.ServeHTTP(w, r)
		return
	}
	authBackend := InitJWT()
	token, err := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return authBackend.PublicKey, nil
	})
	if err != nil || !token.Valid {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	h.handler.ServeHTTP(w, r)
}
