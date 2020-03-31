/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package target

import (
	"crypto/sha512"
	"hash"

	"github.com/minio/sha256-simd"
	"github.com/xdg/scram"
)

// KafkaSHA256 is a function that returns a crypto/sha256 hasher and should be used
// to create Client objects configured for SHA-256 hashing.
var KafkaSHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// KafkaSHA512 is a function that returns a crypto/sha512 hasher and should be used
// to create Client objects configured for SHA-512 hashing.
var KafkaSHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

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
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step takes a string provided from a server (or just an empty string for the
// very first conversation step) and attempts to move the authentication
// conversation forward.  It returns a string to be sent to the server or an
// error if the server message is invalid.  Calling Step after a conversation
// completes is also an error.
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done returns true if the conversation is completed or has errored.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
