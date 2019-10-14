/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
 *
 */

package madmin

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
)

var encryptDataTests = []struct {
	Password string
	Data     []byte
}{
	{Password: "", Data: nil},
	{Password: "", Data: make([]byte, 256)},
	{Password: `xPl.8/rhR"Q_1xLt`, Data: make([]byte, 32)},
	{Password: "m69?yz4W-!k+7p0", Data: make([]byte, 1024*1024)},
	{Password: `7h5oU4$te{;K}fgqlI^]`, Data: make([]byte, 256)},
}

func TestEncryptData(t *testing.T) {
	for i, test := range encryptDataTests {
		i, test := i, test
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			ciphertext, err := EncryptData(test.Password, test.Data)
			if err != nil {
				t.Fatalf("Failed to encrypt data: %v", err)
			}
			plaintext, err := DecryptData(test.Password, bytes.NewReader(ciphertext))
			if err != nil {
				t.Fatalf("Failed to decrypt data: %v", err)
			}
			if !bytes.Equal(plaintext, test.Data) {
				t.Fatal("Decrypt plaintext does not match origin data")
			}
		})
	}
}

var decryptDataTests = []struct {
	Password string
	Data     string
}{
	{Password: "", Data: "828aa81599df0651c0461adb82283e8b89956baee9f6e719947ef9cddc849028001dc9d3ac0938f66b07bacc9751437e1985f8a9763c240e81"},

	{Password: "", Data: "1793c71df6647860437134073c15688cbb15961dc0758c7ee1225e66e79c724c00d790dba9c671eae89da2c736d858286ac9bd027abacc6443" +
		"0375cd41b63b67c070c7fba475a8dd66ae65ba905176c48cbe6f734fc74df87343d8ccff54bada4aeb0a04bd021633ebe6c4768e23f5dea142" +
		"561d4fe3f90ed59d13dc5fb3a585dadec1742325291b9c81692bdd3420b2428127f8195e0ecd9a1c9237712ed67af7339fbbf7ff3ee1c516e1" +
		"f81e69d933e057b30997e7274a2c9698e07c39f0e8d6818858f34c8191871b5a52bea9061806bd029024bfc1d9c1f230904968d6c9e10fddcb" +
		"c006ba97356ff243570fd96df07dd6894e215a6b24c4ed730369519289ebd877aff6ccbd2265985e4ab1a2b7930bab9cfb767b97348a639ddf" +
		"8db81bf5151da7e8f3d9638a1b86eb1dd78cc6a526f10a414c78638f"},

	{Password: `xPl.8/rhR"Q_1xLt`, Data: "b5c016e93b84b473fc8a37af94936563630c36d6df1841d23a86ee51ca161f9e00ac19116b32f643ff6a56a212b265d8c56" +
		"195bb0d12ce199e13dfdc5272f80c1564da2c6fc2fa18da91d8062de02af5cdafea491c6f3cae1f"},

	{Password: `7h5oU4$te{;K}fgqlI^]`, Data: "c58edf7cfd557b6b655de6f48b1a3049d8d049dadb3a7bfa9ac9ccbb5baf37ec00f83086a26f43b7d6bc9075ad0" +
		"38bf5741f118d502ebe94165e4072ba7f98535d6b1e3b6ae67a98115d146d9b4d90e4df4ae82df9cfa17ed7cd42" +
		"465181559f7ddf09c98beec521bb4478e0cb73c4e0827af8688ff4e7a07327a10d5a180035e6ddb16d974a85257" +
		"981cd9e0360a20f7b4d653190267dfb241148f018ae180568042e864b9e1b5bc05425a3abc2b0324f50c72d5679" +
		"8f924405dfc0f8523f4bb564ed65af8e1b1c82a7a0640552ecf81985d95d0993d99172592ddc1393dfa63e8f0b3" +
		"d744b2cc4b73384ca4693f0c1aec0e9b00e85f2937e891105d67da8f59c14ca96608e0425c42f9c1e7c2a8b3413" +
		"e1381784f9cfe01de7c47cea1f8d7a7d88f5d4aca783cf55332b47f957a6b9a65269d7eb606b877b"},
}

func TestDecryptData(t *testing.T) {
	for i, test := range decryptDataTests {
		i, test := i, test
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			ciphertext, err := hex.DecodeString(test.Data)
			if err != nil {
				t.Fatalf("Failed to decode ciphertext data: %v", err)
			}
			_, err = DecryptData(test.Password, bytes.NewReader(ciphertext))
			if err != nil {
				t.Fatalf("Failed to decrypt data: %v", err)
			}
		})
	}
}
