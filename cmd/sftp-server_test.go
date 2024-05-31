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

				suite.SFTP_ValidUserLoginWithServiceAccount(c)

				// LDAP
				ldapServer := os.Getenv(EnvTestLDAPServer)
				if ldapServer == "" {
					c.Skipf("Skipping LDAP test as no LDAP server is provided via %s", EnvTestLDAPServer)
				}

				suite.SetUpLDAP(c, ldapServer)

				suite.SFTP_LDAP_FailedAuthenticationDueToMissingPolicy(c)
				suite.SFTP_LDAP_FailedAuthenticationDueToInvalidUser(c)
				suite.SFTP_LDAP_FailedForcedServiceAccountAuthOnLDAPUser(c)

				suite.SFTP_LDAP_ValidUserLoginWithPassword(c)

				// Public Key Authentication
				suite.SFTP_LDAP_PublicKeyAuthentication(c)
				suite.SFTP_LDAP_FailedPublicKeyAuthenticationInvalidKey(c)

				suite.TearDownSuite(c)
			},
		)
	}
}

func (s *TestSuiteIAM) SFTP_LDAP_FailedPublicKeyAuthenticationInvalidKey(c *check) {
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

func (s *TestSuiteIAM) SFTP_LDAP_PublicKeyAuthentication(c *check) {
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

func (s *TestSuiteIAM) SFTP_LDAP_FailedAuthenticationDueToMissingPolicy(c *check) {
	newSSHCon := newSSHConnMock("dillon=ldap")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon"))
	if err == nil || !errors.Is(err, sftpErrUserHasNoPolicies) {
		c.Fatalf("expected err(%s) but got (%s)", sftpErrUserHasNoPolicies, err)
	}

	newSSHCon = newSSHConnMock("dillon")
	_, err = sshPasswordAuth(newSSHCon, []byte("dillon"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTP_LDAP_FailedAuthenticationDueToInvalidUser(c *check) {
	newSSHCon := newSSHConnMock("dillon_error")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon_error"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTP_LDAP_FailedForcedServiceAccountAuthOnLDAPUser(c *check) {
	newSSHCon := newSSHConnMock("dillon=svc")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon"))
	if err == nil || !errors.Is(err, errNoSuchUser) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTP_LDAP_FailedAuthenticationDueToInvalidPassword(c *check) {
	newSSHCon := newSSHConnMock("dillon")
	_, err := sshPasswordAuth(newSSHCon, []byte("dillon_error"))
	if err == nil || !errors.Is(err, errAuthentication) {
		c.Fatalf("expected err(%s) but got (%s)", errNoSuchUser, err)
	}
}

func (s *TestSuiteIAM) SFTP_ValidUserLoginWithServiceAccount(c *check) {
	ctx, cancel := context.WithTimeout(context.Background(), testDefaultTimeout)
	defer cancel()

	accessKey, secretKey := mustGenerateCredentials(c)
	err := s.adm.SetUser(ctx, accessKey, secretKey, madmin.AccountEnabled)
	if err != nil {
		c.Fatalf("Unable to set user: %v", err)
	}

	err = s.adm.SetPolicy(ctx, "readwrite", accessKey, false)
	if err != nil {
		c.Fatalf("unable to set policy: %v", err)
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

func (s *TestSuiteIAM) SFTP_LDAP_ValidUserLoginWithPassword(c *check) {
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

	userDN := "uid=dillon,ou=people,ou=swengg,dc=min,dc=io"
	err = s.adm.SetPolicy(ctx, policy, userDN, false)
	if err != nil {
		c.Fatalf("Unable to set policy: %v", err)
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
