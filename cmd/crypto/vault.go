// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	vault "github.com/hashicorp/vault/api"
)

const (
	// vaultEndpointEnv Vault endpoint environment variable
	vaultEndpointEnv = "MINIO_SSE_VAULT_ENDPOINT"
	// vaultAuthTypeEnv type of vault auth to be used
	vaultAuthTypeEnv = "MINIO_SSE_VAULT_AUTH_TYPE"
	// vaultAppRoleIDEnv  Vault AppRole ID environment variable
	vaultAppRoleIDEnv = "MINIO_SSE_VAULT_APPROLE_ID"
	// vaultAppSecretIDEnv Vault AppRole Secret environment variable
	vaultAppSecretIDEnv = "MINIO_SSE_VAULT_APPROLE_SECRET"
	// vaultKeyVersionEnv Vault Key Version environment variable
	vaultKeyVersionEnv = "MINIO_SSE_VAULT_KEY_VERSION"
	// vaultKeyNameEnv Vault Encryption Key Name environment variable
	vaultKeyNameEnv = "MINIO_SSE_VAULT_KEY_NAME"

	// vaultCAPath is the path to a directory of PEM-encoded CA
	// cert files to verify the Vault server SSL certificate.
	vaultCAPath = "MINIO_SSE_VAULT_CAPATH"
)

var (
	//ErrKMSAuthLogin is raised when there is a failure authenticating to KMS
	ErrKMSAuthLogin = errors.New("Vault service did not return auth info")
)

type vaultService struct {
	config        *VaultConfig
	client        *vault.Client
	leaseDuration time.Duration
}

// return transit secret engine's path for generate data key operation
func (v *vaultService) genDataKeyEndpoint(key string) string {
	return "/transit/datakey/plaintext/" + key
}

// return transit secret engine's path for decrypt operation
func (v *vaultService) decryptEndpoint(key string) string {
	return "/transit/decrypt/" + key
}

// VaultKey represents vault encryption key-id name & version
type VaultKey struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// VaultAuth represents vault auth type to use. For now, AppRole is the only supported
// auth type.
type VaultAuth struct {
	Type    string       `json:"type"`
	AppRole VaultAppRole `json:"approle"`
}

// VaultAppRole represents vault approle credentials
type VaultAppRole struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// VaultConfig holds config required to start vault service
type VaultConfig struct {
	Endpoint string    `json:"endpoint"`
	Auth     VaultAuth `json:"auth"`
	Key      VaultKey  `json:"key-id"`
}

// validate whether all required env variables needed to start vault service have
// been set
func validateVaultConfig(c *VaultConfig) error {
	if c.Endpoint == "" {
		return fmt.Errorf("Missing hashicorp vault endpoint - %s is empty", vaultEndpointEnv)
	}
	if strings.ToLower(c.Auth.Type) != "approle" {
		return fmt.Errorf("Unsupported hashicorp vault auth type - %s", vaultAuthTypeEnv)
	}
	if c.Auth.AppRole.ID == "" {
		return fmt.Errorf("Missing hashicorp vault AppRole ID - %s is empty", vaultAppRoleIDEnv)
	}
	if c.Auth.AppRole.Secret == "" {
		return fmt.Errorf("Missing hashicorp vault AppSecret ID - %s is empty", vaultAppSecretIDEnv)
	}
	if c.Key.Name == "" {
		return fmt.Errorf("Invalid value set in environment variable %s", vaultKeyNameEnv)
	}
	if c.Key.Version < 0 {
		return fmt.Errorf("Invalid value set in environment variable %s", vaultKeyVersionEnv)
	}
	return nil
}

// authenticate to vault with app role id and app role secret, and get a client access token, lease duration
func getVaultAccessToken(client *vault.Client, appRoleID, appSecret string) (token string, duration int, err error) {
	data := map[string]interface{}{
		"role_id":   appRoleID,
		"secret_id": appSecret,
	}
	resp, e := client.Logical().Write("auth/approle/login", data)
	if e != nil {
		return token, duration, e
	}
	if resp.Auth == nil {
		return token, duration, ErrKMSAuthLogin
	}
	return resp.Auth.ClientToken, resp.Auth.LeaseDuration, nil
}

// NewVaultConfig sets KMSConfig from environment
// variables and performs validations.
func NewVaultConfig() (KMSConfig, error) {
	kc := KMSConfig{}
	endpoint := os.Getenv(vaultEndpointEnv)
	roleID := os.Getenv(vaultAppRoleIDEnv)
	roleSecret := os.Getenv(vaultAppSecretIDEnv)
	keyName := os.Getenv(vaultKeyNameEnv)
	keyVersion := 0
	authType := "approle"
	if versionStr := os.Getenv(vaultKeyVersionEnv); versionStr != "" {
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return kc, fmt.Errorf("Unable to parse %s value (`%s`)", vaultKeyVersionEnv, versionStr)
		}
		keyVersion = version
	}
	// return if none of the vault env variables are configured
	if (endpoint == "") && (roleID == "") && (roleSecret == "") && (keyName == "") && (keyVersion == 0) {
		return kc, nil
	}
	c := VaultConfig{
		Endpoint: endpoint,
		Auth: VaultAuth{
			Type: authType,
			AppRole: VaultAppRole{
				ID:     roleID,
				Secret: roleSecret,
			},
		},
		Key: VaultKey{
			Version: keyVersion,
			Name:    keyName,
		},
	}
	if err := validateVaultConfig(&c); err != nil {
		return kc, err
	}
	kc.Vault = c
	return kc, nil
}

// NewVault initializes Hashicorp Vault KMS by
// authenticating to Vault with the credentials in KMSConfig,
// and gets a client token for future api calls.
func NewVault(kmsConf KMSConfig) (KMS, error) {
	config := kmsConf.Vault
	vconfig := &vault.Config{
		Address: config.Endpoint,
	}
	if err := vconfig.ConfigureTLS(&vault.TLSConfig{
		CAPath: os.Getenv(vaultCAPath),
	}); err != nil {
		return nil, err
	}
	c, err := vault.NewClient(vconfig)
	if err != nil {
		return nil, err
	}
	accessToken, leaseDuration, err := getVaultAccessToken(c, config.Auth.AppRole.ID, config.Auth.AppRole.Secret)
	if err != nil {
		return nil, err
	}
	// authenticate and get the access token
	c.SetToken(accessToken)
	v := vaultService{client: c, config: &config, leaseDuration: time.Duration(leaseDuration)}
	v.renewToken(c)
	return &v, nil
}

func (v *vaultService) renewToken(c *vault.Client) {
	retryDelay := 1 * time.Minute
	go func() {
		for {
			s, err := c.Auth().Token().RenewSelf(int(v.leaseDuration))
			if err != nil {
				time.Sleep(retryDelay)
				continue
			}
			nextRenew := s.Auth.LeaseDuration / 2
			time.Sleep(time.Duration(nextRenew) * time.Second)
		}
	}()
}

// Generates a random plain text key, sealed plain text key from
// Vault. It returns the plaintext key and sealed plaintext key on success
func (v *vaultService) GenerateKey(keyID string, ctx Context) (key [32]byte, sealedKey []byte, err error) {
	contextStream := new(bytes.Buffer)
	ctx.WriteTo(contextStream)

	payload := map[string]interface{}{
		"context": base64.StdEncoding.EncodeToString(contextStream.Bytes()),
	}
	s, err1 := v.client.Logical().Write(v.genDataKeyEndpoint(keyID), payload)

	if err1 != nil {
		return key, sealedKey, err1
	}
	sealKey := s.Data["ciphertext"].(string)
	plainKey, err := base64.StdEncoding.DecodeString(s.Data["plaintext"].(string))
	if err != nil {
		return key, sealedKey, err1
	}
	copy(key[:], []byte(plainKey))
	return key, []byte(sealKey), nil
}

// unsealKMSKey unseals the sealedKey using the Vault master key
// referenced by the keyID. The plain text key is returned on success.
func (v *vaultService) UnsealKey(keyID string, sealedKey []byte, ctx Context) (key [32]byte, err error) {
	contextStream := new(bytes.Buffer)
	ctx.WriteTo(contextStream)
	payload := map[string]interface{}{
		"ciphertext": string(sealedKey),
		"context":    base64.StdEncoding.EncodeToString(contextStream.Bytes()),
	}
	s, err1 := v.client.Logical().Write(v.decryptEndpoint(keyID), payload)
	if err1 != nil {
		return key, err1
	}
	base64Key := s.Data["plaintext"].(string)
	plainKey, err1 := base64.StdEncoding.DecodeString(base64Key)
	if err1 != nil {
		return key, err1
	}
	copy(key[:], []byte(plainKey))

	return key, nil
}
