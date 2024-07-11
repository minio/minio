// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
)

// Token - parses the output from IDP id_token.
type Token struct {
	AccessToken string `json:"access_token"`
	Expiry      int    `json:"expires_in"`
}

// KeycloakProvider implements Provider interface for KeyCloak Identity Provider.
type KeycloakProvider struct {
	sync.Mutex

	oeConfig DiscoveryDoc
	client   http.Client
	adminURL string
	realm    string

	// internal value refreshed
	accessToken Token
}

// LoginWithUser authenticates username/password, not needed for Keycloak
func (k *KeycloakProvider) LoginWithUser(username, password string) error {
	return ErrNotImplemented
}

// LoginWithClientID is implemented by Keycloak service account support
func (k *KeycloakProvider) LoginWithClientID(clientID, clientSecret string) error {
	values := url.Values{}
	values.Set("client_id", clientID)
	values.Set("client_secret", clientSecret)
	values.Set("grant_type", "client_credentials")

	req, err := http.NewRequest(http.MethodPost, k.oeConfig.TokenEndpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var accessToken Token
	if err = json.NewDecoder(resp.Body).Decode(&accessToken); err != nil {
		return err
	}

	k.Lock()
	k.accessToken = accessToken
	k.Unlock()
	return nil
}

// LookupUser lookup user by their userid.
func (k *KeycloakProvider) LookupUser(userid string) (User, error) {
	req, err := http.NewRequest(http.MethodGet, k.adminURL, nil)
	if err != nil {
		return User{}, err
	}
	req.URL.Path = path.Join(req.URL.Path, "realms", k.realm, "users", userid)

	k.Lock()
	accessToken := k.accessToken
	k.Unlock()
	if accessToken.AccessToken == "" {
		return User{}, ErrAccessTokenExpired
	}
	req.Header.Set("Authorization", "Bearer "+accessToken.AccessToken)
	resp, err := k.client.Do(req)
	if err != nil {
		return User{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		var u User
		if err = json.NewDecoder(resp.Body).Decode(&u); err != nil {
			return User{}, err
		}
		return u, nil
	case http.StatusNotFound:
		return User{
			ID:      userid,
			Enabled: false,
		}, nil
	case http.StatusUnauthorized:
		return User{}, ErrAccessTokenExpired
	}
	return User{}, fmt.Errorf("Unable to lookup - keycloak user lookup returned %v", resp.Status)
}

// Option is a function type that accepts a pointer Target
type Option func(*KeycloakProvider)

// WithTransport provide custom transport
func WithTransport(transport http.RoundTripper) Option {
	return func(p *KeycloakProvider) {
		p.client = http.Client{
			Transport: transport,
		}
	}
}

// WithOpenIDConfig provide OpenID Endpoint configuration discovery document
func WithOpenIDConfig(oeConfig DiscoveryDoc) Option {
	return func(p *KeycloakProvider) {
		p.oeConfig = oeConfig
	}
}

// WithAdminURL provide admin URL configuration for Keycloak
func WithAdminURL(url string) Option {
	return func(p *KeycloakProvider) {
		p.adminURL = url
	}
}

// WithRealm provide realm configuration for Keycloak
func WithRealm(realm string) Option {
	return func(p *KeycloakProvider) {
		p.realm = realm
	}
}

// KeyCloak initializes a new keycloak provider
func KeyCloak(opts ...Option) (Provider, error) {
	p := &KeycloakProvider{}

	for _, opt := range opts {
		opt(p)
	}

	if p.adminURL == "" {
		return nil, errors.New("Admin URL cannot be empty")
	}

	_, err := url.Parse(p.adminURL)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse the adminURL %s: %w", p.adminURL, err)
	}

	if p.client.Transport == nil {
		p.client.Transport = http.DefaultTransport
	}

	if p.oeConfig.TokenEndpoint == "" {
		return nil, errors.New("missing OpenID token endpoint")
	}

	if p.realm == "" {
		p.realm = "master" // default realm
	}

	return p, nil
}
