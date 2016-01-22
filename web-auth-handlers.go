package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dgrijalva/jwt-go"
)

func srvLogin(requestUser *User) (int, []byte) {
	authBackend := InitJWT()
	if authBackend.Authenticate(requestUser) {
		token, err := authBackend.GenerateToken(requestUser.Username)
		if err != nil {
			return http.StatusInternalServerError, nil
		}
		response, err := json.Marshal(AuthToken{token})
		if err != nil {
			return http.StatusInternalServerError, nil
		}
		return http.StatusOK, response
	}
	return http.StatusUnauthorized, nil
}

func srvRefreshToken(req *http.Request) (int, []byte) {
	authBackend := InitJWT()
	tokenRequest, err := jwt.ParseFromRequest(req, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return authBackend.PublicKey, nil
	})
	if err != nil {
		return http.StatusInternalServerError, nil
	}
	if tokenRequest.Valid {
		userName, ok := tokenRequest.Claims["sub"].(string)
		if !ok {
			return http.StatusUnauthorized, nil
		}
		token, err := authBackend.GenerateToken(userName)
		if err != nil {
			return http.StatusInternalServerError, nil
		}
		response, err := json.Marshal(AuthToken{token})
		if err != nil {
			return http.StatusInternalServerError, nil
		}
		return http.StatusOK, response
	}
	return http.StatusUnauthorized, nil
}

func srvLogout(req *http.Request) int {
	authBackend := InitJWT()
	tokenRequest, err := jwt.ParseFromRequest(req, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return authBackend.PublicKey, nil
	})
	if err != nil {
		return http.StatusInternalServerError
	}
	if tokenRequest.Valid {
		tokenString := req.Header.Get("Authorization")
		if err = authBackend.Logout(tokenString, tokenRequest); err != nil {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	}
	return http.StatusUnauthorized
}

// LoginHandler - user login handler.
func (web WebAPI) LoginHandler(w http.ResponseWriter, r *http.Request) {
	requestUser := new(User)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestUser); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	responseStatus, token := srvLogin(requestUser)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseStatus)
	w.Write(token)
}

// RefreshTokenHandler - refresh token handler.
func (web WebAPI) RefreshTokenHandler(w http.ResponseWriter, r *http.Request) {
	responseStatus, token := srvRefreshToken(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseStatus)
	w.Write(token)
}

// LogoutHandler - user logout handler.
func (web WebAPI) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	responseStatus := srvLogout(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseStatus)
}
