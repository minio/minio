/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/probe"
)

// AuthService auth service
type AuthService struct{}

// AuthArgs auth params
type AuthArgs struct {
	User string `json:"user"`
}

// AuthReply reply with new access keys and secret ids
type AuthReply struct {
	Name            string `json:"name"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

// generateAuth generate new auth keys for a user
func generateAuth(args *AuthArgs, reply *AuthReply) *probe.Error {
	config, err := auth.LoadConfig()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			// Initialize new config, since config file doesn't exist yet
			config = &auth.Config{}
			config.Version = "0.0.1"
			config.Users = make(map[string]*auth.User)
		} else {
			return err.Trace()
		}
	}
	if _, ok := config.Users[args.User]; ok {
		return probe.NewError(errors.New("Credentials already set, if you wish to change this invoke Reset() method"))
	}
	accessKeyID, err := auth.GenerateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessKeyID)

	secretAccessKey, err := auth.GenerateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretAccessKey)
	reply.Name = args.User

	config.Users[args.User] = &auth.User{
		Name:            args.User,
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	if err := auth.SaveConfig(config); err != nil {
		return err.Trace()
	}
	return nil
}

// fetchAuth fetch auth keys for a user
func fetchAuth(args *AuthArgs, reply *AuthReply) *probe.Error {
	config, err := auth.LoadConfig()
	if err != nil {
		return err.Trace()
	}
	if _, ok := config.Users[args.User]; !ok {
		return probe.NewError(errors.New("User not found"))
	}
	reply.AccessKeyID = config.Users[args.User].AccessKeyID
	reply.SecretAccessKey = config.Users[args.User].SecretAccessKey
	reply.Name = args.User
	return nil
}

// resetAuth reset auth keys for a user
func resetAuth(args *AuthArgs, reply *AuthReply) *probe.Error {
	config, err := auth.LoadConfig()
	if err != nil {
		return err.Trace()
	}
	if _, ok := config.Users[args.User]; !ok {
		return probe.NewError(errors.New("User not found"))
	}
	accessKeyID, err := auth.GenerateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessKeyID)
	secretAccessKey, err := auth.GenerateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretAccessKey)
	reply.Name = args.User

	config.Users[args.User] = &auth.User{
		Name:            args.User,
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	return auth.SaveConfig(config).Trace()
}

// Generate auth keys
func (s *AuthService) Generate(r *http.Request, args *AuthArgs, reply *AuthReply) error {
	if strings.TrimSpace(args.User) == "" {
		return errors.New("Invalid argument")
	}
	if err := generateAuth(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

// Fetch auth keys
func (s *AuthService) Fetch(r *http.Request, args *AuthArgs, reply *AuthReply) error {
	if strings.TrimSpace(args.User) == "" {
		return errors.New("Invalid argument")
	}
	if err := fetchAuth(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

// Reset auth keys, generates new set of auth keys
func (s *AuthService) Reset(r *http.Request, args *AuthArgs, reply *AuthReply) error {
	if strings.TrimSpace(args.User) == "" {
		return errors.New("Invalid argument")
	}
	if err := resetAuth(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}
