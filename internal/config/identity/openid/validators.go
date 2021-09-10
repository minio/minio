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

package openid

import (
	"errors"
	"fmt"
	"sync"
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
	ErrTokenExpired = errors.New("token expired")
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

// NewValidators - creates Validators.
func NewValidators() *Validators {
	return &Validators{providers: make(map[ID]Validator)}
}
