// Copyright (c) 2015-2024 MinIO, Inc.
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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/minio/madmin-go/v3"
	"golang.org/x/crypto/ssh"
)

type MockConnMeta struct {
	username string
}

func (m *MockConnMeta) User() string {
	return m.username
}

func (m *MockConnMeta) SessionID() []byte {
	return []byte{}
}

func (m *MockConnMeta) ClientVersion() []byte {
	return []byte{}
}

func (m *MockConnMeta) ServerVersion() []byte {
	return []byte{}
}

func (m *MockConnMeta) RemoteAddr() net.Addr {
	return nil
}

func (m *MockConnMeta) LocalAddr() net.Addr {
	return nil
}

func newSSHConnMock(username string) ssh.ConnMetadata {
	return &MockConnMeta{username: username}
}

func TestSFTPAuthentication(t *testing.T) {
	for i, testCase := range iamTestSuites {
		t.Run(
			fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.ServerTypeDescription),
			func(t *testing.T) {
				c := &check{t, testCase.serverType}
				suite := testCase

				suite.SetUpSuite(c)

				suite.SFTPServiceAccountLogin(c)
				suite.SFTPInvalidServiceAccountPassword(c)

				// LDAP tests
				ldapServer := os.Getenv(EnvTestLDAPServer)
				if ldapServer == "" {
					c.Skipf("Skipping LDAP test as no LDAP server is provided via %s", EnvTestLDAPServer)
				}

				suite.SetUpLDAP(c, ldapServer)

				suite.SFTPFailedAuthDueToMissingPolicy(c)
				suite.SFTPFailedAuthDueToInvalidUser(c)
				suite.SFTPFailedForcedServiceAccountAuthOnLDAPUser(c)
				suite.SFTPFailedAuthDueToInvalidPassword(c)

				suite.SFTPValidLDAPLoginWithPassword(c)

				suite.SFTPPublicKeyAuthentication(c)
				suite.SFTPFailedPublicKeyAuthenticationInvalidKey(c)
				suite.SFTPPublicKeyAuthNoPubKey(c)

				suite.TearDownSuite(c)
			},
		)
	}
}

func (s *TestSuiteIAM) SFTPFailedPublicKeyAuthenticationInvalidKey(c *check) {
	keyBytes, err := os.ReadFile("./testdata/invalid_test_key.pub")
	if err != nil {
		c.Fatalf("could not read test key file: %s", err)
	}

	testKey, _, _, _, err := ssh.ParseAuthorizedKey(keyBytes)
	if err != nil {
		c.Fatalf("could not parse test key file: %s", err)
	}

	newSSHCon := newSSHConnMock("dillon=ldap")
	_, err = sshPubKeyAuth(newSSHCon, testKey)
	if err == nil || !errors.Is(err, errAuthentication) {
		c.Fatalf("expected err(%s) but got (%s)", errAuthentication, err)
	}

	newSSHCon = newSSHConnMock("dillon")
	_, err = sshPubKeyAuth(newSSHCon, testKey)
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTPPublicKeyAuthentication(c *check) {
	keyBytes, err := os.ReadFile("./testdata/dillon_test_key.pub")
	if err != nil {
		c.Fatalf("could not read test key file: %s", err)
	}

	testKey, _, _, _, err := ssh.ParseAuthorizedKey(keyBytes)
	if err != nil {
		c.Fatalf("could not parse test key file: %s", err)
	}

	newSSHCon := newSSHConnMock("dillon=ldap")
	_, err = sshPubKeyAuth(newSSHCon, testKey)
	if err != nil {
		c.Fatalf("expected no error but got(%s)", err)
	}

	newSSHCon = newSSHConnMock("dillon")
	_, err = sshPubKeyAuth(newSSHCon, testKey)
	if err != nil {
		c.Fatalf("expected no error but got(%s)", err)
	}
}

// A user without an sshpubkey attribute in LDAP (here: fahim) should not be
// able to authenticate.
func (s *TestSuiteIAM) SFTPPublicKeyAuthNoPubKey(c *check) {
	keyBytes, err := os.ReadFile("./testdata/dillon_test_key.pub")
	if err != nil {
		c.Fatalf("could not read test key file: %s", err)
	}

	testKey, _, _, _, err := ssh.ParseAuthorizedKey(keyBytes)
	if err != nil {
		c.Fatalf("could not parse test key file: %s", err)
	}

	newSSHCon := newSSHConnMock("fahim=ldap")
	_, err = sshPubKeyAuth(newSSHCon, testKey)
	if err == nil {
		c.Fatalf("expected error but got none")
	}

	newSSHCon = newSSHConnMock("fahim")
	_, err = sshPubKeyAuth(newSSHCon, testKey)
	if err == nil {
		c.Fatalf("expected error but got none")
	}
}

func (s *TestSuiteIAM) SFTPFailedAuthDueToMissingPolicy(c *check) {
	newSSHCon := newSSHConnMock("dillon=ldap")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon"))
	if err == nil || !errors.Is(err, errSFTPUserHasNoPolicies) {
		c.Fatalf("expected err(%s) but got (%s)", errSFTPUserHasNoPolicies, err)
	}

	newSSHCon = newSSHConnMock("dillon")
	_, err = sshPasswordAuth(newSSHCon, []byte("dillon"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTPFailedAuthDueToInvalidUser(c *check) {
	newSSHCon := newSSHConnMock("dillon_error")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon_error"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTPFailedForcedServiceAccountAuthOnLDAPUser(c *check) {
	newSSHCon := newSSHConnMock("dillon=svc")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTPFailedAuthDueToInvalidPassword(c *check) {
	newSSHCon := newSSHConnMock("dillon")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon_error"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTPInvalidServiceAccountPassword(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	accessKey, secretKey := mustGenerateCredentials(c)
	err := s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	userReq := madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		User:     accessKey,
	}
	if _, err := s.adm.AttachPolicy(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
	}

	newSSHCon := newSSHConnMock(accessKey + "=svc")
	_, err = sshPasswordAuth(newSSHCon, []byte("invalid"))
	if err == nil || !errors.Is(err, errAuthentication) {
		c.Fatalf("expected err(%s) but got (%s)", errAuthentication, err)
	}

	newSSHCon = newSSHConnMock(accessKey)
	_, err = sshPasswordAuth(newSSHCon, []byte("invalid"))
	if err == nil || !errors.Is(err, errAuthentication) {
		c.Fatalf("expected err(%s) but got (%s)", errAuthentication, err)
	}
}

func (s *TestSuiteIAM) SFTPServiceAccountLogin(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	accessKey, secretKey := mustGenerateCredentials(c)
	err := s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	userReq := madmin.PolicyAssociationReq{
		Policies: []string{"readwrite"},
		User:     accessKey,
	}
	if _, err := s.adm.AttachPolicy(ctx, userReq); err != nil {
		c.Fatalf("Unable to attach policy: %v", err)
	}

	newSSHCon := newSSHConnMock(accessKey + "=svc")
	_, err = sshPasswordAuth(newSSHCon, []byte(secretKey))
	if err != nil {
		c.Fatalf("expected no error but got (%s)", err)
	}

	newSSHCon = newSSHConnMock(accessKey)
	_, err = sshPasswordAuth(newSSHCon, []byte(secretKey))
	if err != nil {
		c.Fatalf("expected no error but got (%s)", err)
	}
}

func (s *TestSuiteIAM) SFTPValidLDAPLoginWithPassword(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	// we need to do this so that the user has a policy before authentication.
	// ldap user accounts without policies are denied access in sftp.
	policy := "mypolicy"
	policyBytes := []byte(`{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:ListBucket"
   ],
   "Resource": [
    "arn:aws:s3:::BUCKET/*"
   ]
  }
 ]
}`)

	err := s.adm.AddCannedPolicy(ctx, policy, policyBytes)
	if err != nil {
		c.Fatalf("policy add error: %v", err)
	}

	{
		userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
		userReq := madmin.PolicyAssociationReq{
			Policies: []string{policy},
			User:     userDN,
		}
		if _, err := s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
			c.Fatalf("Unable to attach policy: %v", err)
		}

		newSSHCon := newSSHConnMock("dillon=ldap")
		_, err = sshPasswordAuth(newSSHCon, []byte("dillon"))
		if err != nil {
			c.Fatal("Password authentication failed for user (dillon):", err)
		}

		newSSHCon = newSSHConnMock("dillon")
		_, err = sshPasswordAuth(newSSHCon, []byte("dillon"))
		if err != nil {
			c.Fatal("Password authentication failed for user (dillon):", err)
		}
	}
	{
		userDN := "uid=fahim,ou=people,ou=swengg,dc=min,dc=io"
		userReq := madmin.PolicyAssociationReq{
			Policies: []string{policy},
			User:     userDN,
		}
		if _, err := s.adm.AttachPolicyLDAP(ctx, userReq); err != nil {
			c.Fatalf("Unable to attach policy: %v", err)
		}

		newSSHCon := newSSHConnMock("fahim=ldap")
		_, err = sshPasswordAuth(newSSHCon, []byte("fahim"))
		if err != nil {
			c.Fatal("Password authentication failed for user (fahim):", err)
		}

		newSSHCon = newSSHConnMock("fahim")
		_, err = sshPasswordAuth(newSSHCon, []byte("fahim"))
		if err != nil {
			c.Fatal("Password authentication failed for user (fahim):", err)
		}
	}
}
