package main

import (
	"fmt"
	"net/http"

	jwtgo "github.com/dgrijalva/jwt-go"
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
	// Let the top level caller handle if the requests should be
	// allowed.
	if r.Header.Get("Authorization") == "" {
		h.handler.ServeHTTP(w, r)
		return
	}
	jwt := InitJWT()
	token, err := jwtgo.ParseFromRequest(r, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return jwt.PublicKey, nil
	})
	if err != nil || !token.Valid {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	h.handler.ServeHTTP(w, r)
}
