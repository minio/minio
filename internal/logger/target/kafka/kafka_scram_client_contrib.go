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

package kafka

import (
	"crypto/sha512"
	"strings"

	"github.com/IBM/sarama"
	"github.com/xdg/scram"

	"github.com/minio/minio/internal/hash/sha256"
)

func initScramClient(cfg Config, config *sarama.Config) {
	switch strings.ToLower(cfg.SASL.Mechanism) {
	case "sha512":
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: KafkaSHA512} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	case "sha256":
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: KafkaSHA256} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
	default:
		// default to PLAIN
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypePlaintext)
	}
}

// KafkaSHA256 is a function that returns a crypto/sha256 hasher and should be used
// to create Client objects configured for SHA-256 hashing.
var KafkaSHA256 scram.HashGeneratorFcn = sha256.New

// KafkaSHA512 is a function that returns a crypto/sha512 hasher and should be used
// to create Client objects configured for SHA-512 hashing.
var KafkaSHA512 scram.HashGeneratorFcn = sha512.New

// XDGSCRAMClient implements the client-side of an authentication
// conversation with a server.  A new conversation must be created for
// each authentication attempt.
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin constructs a SCRAM client component based on a given hash.Hash
// factory receiver.  This constructor will normalize the username, password
// and authzID via the SASLprep algorithm, as recommended by RFC-5802.  If
// SASLprep fails, the method returns an error.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.NewConversation()
	return nil
}

// Step takes a string provided from a server (or just an empty string for the
// very first conversation step) and attempts to move the authentication
// conversation forward.  It returns a string to be sent to the server or an
// error if the server message is invalid.  Calling Step after a conversation
// completes is also an error.
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return response, err
}

// Done returns true if the conversation is completed or has errored.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
