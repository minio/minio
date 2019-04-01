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

package openid

import (
	"errors"
	"fmt"
	"sync"

	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
	config "github.com/minio/minio/pkg/server-config"
)

// ID - holds identification name authentication validator target.
type ID string

// Validator interface describes basic implementation
// requirements of various authentication providers.
type Validator interface {
	// Validate is a custom validator function for this provider,
	// each validation is authenticationType or provider specific.
	Validate(token string, duration string) (map[string]interface{}, error)

	// ID returns provider name of this provider.
	ID() ID
}

// ErrTokenExpired - error token expired
var (
	ErrTokenExpired    = errors.New("token expired")
	ErrInvalidDuration = errors.New("duration higher than token expiry")
)

// Validators - holds list of providers indexed by provider id.
type Validators struct {
	sync.RWMutex
	providers map[ID]Validator
}

// Add - adds unique provider to provider list.
func (list *Validators) Add(provider Validator) error {
	list.Lock()
	defer list.Unlock()

	if _, ok := list.providers[provider.ID()]; ok {
		return fmt.Errorf("provider %v already exists", provider.ID())
	}

	list.providers[provider.ID()] = provider
	return nil
}

// List - returns available provider IDs.
func (list *Validators) List() []ID {
	list.RLock()
	defer list.RUnlock()

	keys := []ID{}
	for k := range list.providers {
		keys = append(keys, k)
	}

	return keys
}

// Get - returns the provider for the given providerID, if not found
// returns an error.
func (list *Validators) Get(id ID) (p Validator, err error) {
	list.RLock()
	defer list.RUnlock()
	var ok bool
	if p, ok = list.providers[id]; !ok {
		return nil, fmt.Errorf("provider %v doesn't exist", id)
	}
	return p, nil
}

// OpenID JWKS URL.
const (
	EnvIdentityOpenIDJWKSURL = "MINIO_IDENTITY_OPENID_JWKS_URL"
)

// NewValidators - initialize new OpenID validators
func NewValidators(kvs config.KVS) (*Validators, error) {
	if kvs.Get(config.State) != config.StateEnabled {
		return nil, nil
	}
	jwksURL := env.Get(EnvIdentityOpenIDJWKSURL, kvs.Get(JwksURL))
	u, err := xnet.ParseURL(jwksURL)
	if err != nil {
		return nil, err
	}
	jwks := JWKSArgs{
		URL: u,
	}
	// JWKS url is configured, try to populate public key.
	if err = jwks.PopulatePublicKey(); err != nil {
		return nil, fmt.Errorf("Unable to populate public key from JWKS URL %s: %s", jwksURL, err)
	}

	validators := &Validators{providers: make(map[ID]Validator)}
	validators.Add(NewJWT(jwks))
	return validators, nil
}
