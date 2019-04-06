// +build ignore

/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
	googleOAuth2 "golang.org/x/oauth2/google"

	"github.com/minio/minio/pkg/auth"
)

// AssumedRoleUser - The identifiers for the temporary security credentials that
// the operation returns. Please also see https://docs.aws.amazon.com/goto/WebAPI/sts-2011-06-15/AssumedRoleUser
type AssumedRoleUser struct {
	Arn           string
	AssumedRoleID string `xml:"AssumeRoleId"`
	// contains filtered or unexported fields
}

// AssumeRoleWithWebIdentityResponse contains the result of successful AssumeRoleWithWebIdentity request.
type AssumeRoleWithWebIdentityResponse struct {
	XMLName          xml.Name          `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleWithWebIdentityResponse" json:"-"`
	Result           WebIdentityResult `xml:"AssumeRoleWithWebIdentityResult"`
	ResponseMetadata struct {
		RequestID string `xml:"RequestId,omitempty"`
	} `xml:"ResponseMetadata,omitempty"`
}

// WebIdentityResult - Contains the response to a successful AssumeRoleWithWebIdentity
// request, including temporary credentials that can be used to make MinIO API requests.
type WebIdentityResult struct {
	AssumedRoleUser             AssumedRoleUser  `xml:",omitempty"`
	Audience                    string           `xml:",omitempty"`
	Credentials                 auth.Credentials `xml:",omitempty"`
	PackedPolicySize            int              `xml:",omitempty"`
	Provider                    string           `xml:",omitempty"`
	SubjectFromWebIdentityToken string           `xml:",omitempty"`
}

// Returns a base64 encoded random 32 byte string.
func randomState() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

var (
	stsEndpoint   string
	authEndpoint  string
	tokenEndpoint string
	clientID      string
	clientSecret  string
)

func init() {
	flag.StringVar(&stsEndpoint, "sts-ep", "http://localhost:9000", "STS endpoint")
	flag.StringVar(&authEndpoint, "auth-ep", googleOAuth2.Endpoint.AuthURL, "Auth endpoint")
	flag.StringVar(&tokenEndpoint, "token-ep", googleOAuth2.Endpoint.TokenURL, "Token endpoint")
	flag.StringVar(&clientID, "cid", "", "Client ID")
	flag.StringVar(&clientSecret, "csec", "", "Client secret")
}

func main() {
	flag.Parse()
	if clientID == "" || clientSecret == "" {
		flag.PrintDefaults()
		return
	}

	ctx := context.Background()

	config := oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  authEndpoint,
			TokenURL: tokenEndpoint,
		},
		RedirectURL: "http://localhost:8080/oauth2/callback",
		Scopes:      []string{"openid", "profile", "email"},
	}

	state := randomState()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, config.AuthCodeURL(state), http.StatusFound)
	})

	http.HandleFunc("/oauth2/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != state {
			http.Error(w, "state did not match", http.StatusBadRequest)
			return
		}

		oauth2Token, err := config.Exchange(ctx, r.URL.Query().Get("code"))
		if err != nil {
			http.Error(w, "Failed to exchange token: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if oauth2Token.Valid() {
			v := url.Values{}
			v.Set("Action", "AssumeRoleWithWebIdentity")
			v.Set("WebIdentityToken", fmt.Sprintf("%s", oauth2Token.Extra("id_token")))
			v.Set("DurationSeconds", fmt.Sprintf("%d", int64(oauth2Token.Expiry.Sub(time.Now().UTC()).Seconds())))
			v.Set("Version", "2011-06-15")

			u, err := url.Parse("http://localhost:9000")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			u.RawQuery = v.Encode()

			req, err := http.NewRequest("POST", u.String(), nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return

			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				http.Error(w, resp.Status, resp.StatusCode)
				return
			}

			a := AssumeRoleWithWebIdentityResponse{}
			if err = xml.NewDecoder(resp.Body).Decode(&a); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Write([]byte("##### Credentials\n"))
			c, err := json.MarshalIndent(a.Result.Credentials, "", "\t")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(c)
		}
	})

	log.Printf("listening on http://%s/", "localhost:8080")
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
