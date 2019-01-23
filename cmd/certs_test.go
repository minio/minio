/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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

package cmd

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var loadPrivateKeyTests = []struct {
	FileName   string
	Password   string
	PrivateKey string
}{
	{
		FileName: "ec-key-1",
		Password: "",
		PrivateKey: `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIHAWTq9/ShGA3zkepzmow3UX2qqb87Y1YgVPtqfp0OjxoAoGCCqGSM49
AwEHoUQDQgAE8ZQHj+aBL6ekZSaq+gIsBRkftE5yi69GaqgA+bqOGS9ouSLfgoO8
7AuoPGPb0SbbhcsZeWdO09vwPmF2Psgi7Q==
-----END EC PRIVATE KEY-----
`,
	},
	{
		FileName: "ec-encrypted-key-1",
		Password: "PASSWORD",
		PrivateKey: `-----BEGIN EC PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-256-CBC,35616464EE87B8114FAAB5903EABB2D4

GX6E3CThLoBvVfkLwpz27lsTx70y4ujmQ/oBRDEzYddHkotS1tWeg3aM/9hOMoDp
u6Maqcj2ERphyClShDNC5VPz3an5cvsfXr3rUV2SQQTyQNozcY4+cmCLWNIzuGJl
JL20VYkBVVyo+ia4t3ANlybIVGGDd64DsDOCtEZXPc4=
-----END EC PRIVATE KEY-----
`,
	},
	{
		FileName: "rsa-key-1",
		Password: "",
		PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAtVlgwUEUeLRX/xSph4gIBz7ffPfGAhFeuycsTeXomY2idNf/
caWE68x7ZaCOsP27psI0oGJEM8Zl/beDbvtHvq4q8jR2l6k5A6PVohVdpyQqQ5Hn
xRi5rmjveDkxUBiUpLs+/5u+ZVHNQLvjnS78vFH/faQS2WajbEd8sMEYsYDn0xVU
SQcpCP5HGsPF+9zAbrIatD9O5pxJ9t/RdDPolitLBBxSmYjLlaVNQkNZMlaHvqfi
HLrEMruj42Ccr0JZ8d/VKmpvBArKiOzrfXZ52mrPezgIkne84d9SlYE2k1rnwI6z
hRXa4n+r3AdGgFhLm+kmcMpgEGMpSnIdhfCpTQIDAQABAoIBAHKme/YRx/h7w7o/
fOJzjOfPxwlBuExsYOCfZnIlLQWPNqr6N8PIqh1NXKImfK/G6lEGLLFNDuNQxgA+
efi7ImOZxwrzQhql4Ka9eH2NVnUp9xJa9xVziUiLjQIL3nJN6AOxYaXF9/wkWEhZ
wRAJubzxdj4fcc9CBYWaOGmi2pK05NmHLuAQ4WVTXPJRdKdspgSsWpd1LY/xP3im
Ygdy2rQqsXE3M7WplsUNA+AxDR/zzg+yxf21ogC8cNVZR2sHwwWXLOCrqGB/6Vx/
MoF4+6Ju8nUdQ4ApoJe0kTJXMPHYnUba1J1MU3L7fQhYVWyNf9zg3UM3Vc+Npk8Y
00Rx750CgYEA3DpSYgvQghYapckQFBCYUTQuBdSNsjGcIBiIahl9ejVk8GSEcjQX
C/SL9b/KADEh2g9xzGh9E+RAam1v7Gqac3JEcdN/g8Pc07OclxLrkWp/ug793pz5
vfE6Bndo49uZRI28Qo0tiRi0afCWm58MDWsO1E6BGPkTzib1fFlC15cCgYEA0s5f
mnndV9R0Y1fUI6ZxBFLWx/gk5kHGsxWo5vA2QqOk1lU5GkqCfwcN4YIvHyEYHuLz
yi+FDDYyToM9/CJIbXTfmoc2fV5d3Khw5CbzivB0kMRLEJyLJUIJOz6GO706K3pl
FRwPJDmY1vGOAVHw9pJfRBbgc1feK5CIcnTCArsCgYEA0DN7CyJsP2+yRaWuQ6nk
tnCEShLG2v43wcgvv07V56FvCi2dYXKJj03ku7JTwJaykDsltL/b7+BMXdGUjIfD
+PzAZHQ1C1cyABrAIbtLZbCvjDD8JWd2W/Igj1h4m2JSphLxNmHN+NyYQ8emOv3E
ITNjU6fcOMXRyYXfc23X2YUCgYAMaArmW3+0WJOU+SlKA9So7Xsof6kkSAC6r26m
UMLQvzLHTnKy4mm1siOV/wRo75is0KyKXKuW4WWqizzNpvLeRj+Wp5iEXlZl0x/5
vXUd2zLxBixoyN3DjpRegTqDL4rJ3kUurd0SQ7WECOlTmI/24vxqVHJXN3ei1rnB
CrAUWwKBgAMLi88PAcHgNOiRVD+VxzUQ+GVXcrdzYOQHL8XlP8A6N9rV9YBbnqzM
zaEhqXN8KPIyxapSQG5GbpoGieIjHW4yrz/rLy0tXrc78eau4YoJ5JDOa/4vaFRp
bSqopuI8CRX3V0/dtVlihD79AKSbOJO0dzb3tBY80Ux5uNfUDL9A
-----END RSA PRIVATE KEY-----
`,
	},
	{
		FileName: "rsa-encrypted-key-1",
		Password: "pwd123456",
		PrivateKey: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-256-CBC,68CBA7EAA99B222251386E7E7207D982

He1F82dWhW1RAPTVnZ2MStjEYactVWuDLbPjDZa9G02Mvcy/7I733Wjzag7sI6vX
eKQToiOcZ5iMCnIPIIJ7sIIpk9pZRTxV5DkjQHnyQbTW9x47T1bLK9cUIdBtv22n
lkvoZMdoUaK7fGHbLRJWyLka6xUlhr1R2ve7uRGd3yGPi2xGC5sPOKj8o2OTQYtx
4AcRi4xBLAvLrQjNPVcYfaCXKi7HWc1kvra2cTHsdsrP17L1oOQJYw3PjJjIPl0O
S3/Z6wW2/qfHcU7TBs9fL/K0VAy+wAtzw7d1WFUkUEHQjO3KlDj/dvL2i7EIC2Qy
rmDELd54bcygo35xr2U5HIZhSfwyzJns4LxyiuMlbJCjA0SHywt8UJb4gJduw3gX
YIJkhs56VEhXONAvERiUxs2Tkj5h0eQzQ7p7tvlDaqO3VpdewimbKPkqKtIEZmHo
kGTcBPX+SWdII7JyJUa0TFk/1+GaUwzDNraFTB7H8iQCVbAJ/tf6H1fUrT9n0wCd
SHtdWe5h5d1Ww1Wq3TYfacSZKR3Zv43ByOGS8kQNF9xd3/F4BoQHSMkLb+7bx5J2
xqMe5mpT2x2vb5bSUWC/qPIAWgeStdy8r8Sl001Y6z0Ek3qAvjIOMXNpd8q4YlBP
Kykp0V61oiqsohcarcbhD2+fWWEuzb9BXNXY6S14O1DG8oeLu5jFB+uyYzi/Ov1T
TUvet8KuhouGw6Cj5n9pihxxEZW9oFQiSXTUGzJnOqFyopDx6YdDT57neaAKDkzt
X4qij1oqPGO3DC1znnAXFOlLjzcRoZ0dRK7S/amgF4Og53pt7hTeZFbcweTbPWKV
suDJOloX0g8IXZ5TjIbFJunVxQRjBQ7R0BuSDz+Rx6vuxqCPMtRBDWbq2yRfA5if
ER7Ib2qVL8Z8VnNgDyni4C8u7ZvdXiCrUssFJxs/GFBU7G6wYUyZGxXzT9i208AS
SnY2yVsEN3XK3X6RfIPTLR4AkEGJrbaZl4/XJbYOLkiweRKETpqv/vTQnvhd7TgO
FU6GvqNj/3nkc6dJrERQK8ktegnsuHIuHnDrRN2IChS3PSiO11ceiMGXe2lASGVc
jFxMLERpp0owCbOWgGQ270c1QGujLg/Wl5NWaXoof7JtjcxYktm6AvoZztj5dkEq
v6zZ48KckgTolyVqZdvf0FjeqcY0UUZh9VzcJdkv7y2UQMu6ycn+Jq9Uo5RSoJkP
A1y91yOsQsXilaUVcCgRRat/8I6+wNlB+fOnL094y1PpMRY/J05Mv/i9eGKXjgcH
pQmO4ctvB6p1/ZEK9X/MwDUZnZpsGkFCrIveq7kPitR9H9bmsenQBDW4UxJgFlX9
tKFcymiNXHAwKoRCWCL8Q7GPtFRHuoi+y68/OdsoPeLxxtmG7Kw+dBsZVTJzN081
be+MDtFAdW3msJsx2J0OhzKahg4aL9EbTPT44lkychzKJTLeLXtaLgfY0DnjAXqd
wg+P21bTfYf4/seUj2mYGr/Zb9u5BqUdS0ni1KuKkIIlNM2EfKcbwz4C59J3OG25
+WAvoL7eJWHpgH8J27xuYfGW0xbbLcLaYdSxxdHHYEGD5EUNf0+3ALQYR9h322ER
-----END RSA PRIVATE KEY-----
`,
	},
}

func TestLoadPrivateKey(t *testing.T) {
	defer os.Unsetenv(TLSPrivateKeyPassword)
	for i, test := range loadPrivateKeyTests {
		if test.Password != "" {
			os.Setenv(TLSPrivateKeyPassword, test.Password)
		} else {
			os.Unsetenv(TLSPrivateKeyPassword)
		}

		filename, err := createTempFile(test.FileName, test.PrivateKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to generate tmp file '%s'", i, test.FileName)
		}
		defer os.Remove(filename)
		if _, _, _, err = loadPrivateKey(filename); err != nil {
			t.Fatalf("Test %d: Failed to load private key from '%s': %s", i, test.FileName, err)
		}
	}
}

func TestGeneratePrivateKey(t *testing.T) {
	defer os.Unsetenv(TLSPrivateKeyPassword)
	os.Setenv(TLSPrivateKeyPassword, "PASSWORD")

	filename := filepath.Join(os.TempDir(), "generated-key-1")
	if err := generatePrivateKey(filename); err != nil {
		t.Fatalf("Failed to generate private key: %s", err)
	}
	defer os.Remove(filename)
	if _, _, _, err := loadPrivateKey(filename); err != nil {
		t.Fatalf("Failed to load private key from '%s': %s", filename, err)
	}
}

func createTempFile(prefix, content string) (tempFile string, err error) {
	var tmpfile *os.File

	if tmpfile, err = ioutil.TempFile("", prefix); err != nil {
		return tempFile, err
	}

	if _, err = tmpfile.Write([]byte(content)); err != nil {
		return tempFile, err
	}

	if err = tmpfile.Close(); err != nil {
		return tempFile, err
	}

	tempFile = tmpfile.Name()
	return tempFile, err
}

var decodeCertificatesTests = []struct {
	FileName    string
	Certificate string
}{
	{
		FileName: "certificate-1",
		Certificate: `-----BEGIN CERTIFICATE-----
MIIBozCCAUqgAwIBAgIRAP+TjqXkfhe5taSx89Tch7QwCgYIKoZIzj0EAwIwLTEZ
MBcGA1UEChMQTWluaW8gZGVwbG95bWVudDEQMA4GA1UECxMHdHV4Ym9vazAeFw0x
OTAxMTgyMjUxMDdaFw0yMDAxMTgyMjUxMDdaMC0xGTAXBgNVBAoTEE1pbmlvIGRl
cGxveW1lbnQxEDAOBgNVBAsTB3R1eGJvb2swWTATBgcqhkjOPQIBBggqhkjOPQMB
BwNCAATNS0Si8DFueftTN4fDeVPICmzi3UyFn3PX6wWKNe3Q7bHQqmkxg8Zlygv9
4/f0fGcyneyFBChRiThw7IF5rVWuo0swSTAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0l
BAwwCgYIKwYBBQUHAwEwDAYDVR0TAQH/BAIwADAUBgNVHREEDTALgglsb2NhbGhv
c3QwCgYIKoZIzj0EAwIDRwAwRAIgD4pSpYUiucHx2yhbvXwiyvY7BamObw+BSE3d
HPPbnksCIHSvXXVXQBvIS5H457npqIfQ+TsJ8QlFYXeh5vVjj9b7
-----END CERTIFICATE-----`,
	},
	{
		FileName: "certificate-2",
		Certificate: `-----BEGIN CERTIFICATE-----
MIIGFjCCBP6gAwIBAgIQBK9K/Z3oYiR1oVgVIqWkGzANBgkqhkiG9w0BAQsFADBN
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMScwJQYDVQQDEx5E
aWdpQ2VydCBTSEEyIFNlY3VyZSBTZXJ2ZXIgQ0EwHhcNMTgxMTI3MDAwMDAwWhcN
MTkxMjAyMTIwMDAwWjBfMQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5p
YTESMBAGA1UEBxMJUGFsbyBBbHRvMRQwEgYDVQQKEwtNaW5pbywgSW5jLjERMA8G
A1UEAwwIKi5taW4uaW8wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDI
K4IK/BwAb/Pl/xTEMRhmlqyUUvhTawgOQDuLsYyOxZVRc4ZyattdV5315nYok7Ie
ocSr9jJdPDExK67Kx0NCgkZVkpAczJSRf6Fxx1OxJ38TcIW3s4eNyz3C5/Vtu7v8
D0NndbYtUns9i0DUxG957NOk2Z8qnhUlLdvhCrxfb0owqOEYSYAUHiRJuouWpcKy
26pDNc6ORfubcHkITgMWnIXI/hlVjh3EJTx7F3Nhkv5jAcxTpSZypAvVoI7HrErQ
R50sHsHOx2GTG6hgM/8KmJWv+yrMUj8KM/K5Np1Hw8t/ato26HJMQR1FytrvUU7W
3Z8fQRDUYcBFvjuDi735AgMBAAGjggLeMIIC2jAfBgNVHSMEGDAWgBQPgGEcgjFh
1S8o541GOLQs4cbZ4jAdBgNVHQ4EFgQUEorxGfbgpj4CHqf710+aLh2mw0kwGwYD
VR0RBBQwEoIIKi5taW4uaW+CBm1pbi5pbzAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0l
BBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMGsGA1UdHwRkMGIwL6AtoCuGKWh0dHA6
Ly9jcmwzLmRpZ2ljZXJ0LmNvbS9zc2NhLXNoYTItZzYuY3JsMC+gLaArhilodHRw
Oi8vY3JsNC5kaWdpY2VydC5jb20vc3NjYS1zaGEyLWc2LmNybDBMBgNVHSAERTBD
MDcGCWCGSAGG/WwBATAqMCgGCCsGAQUFBwIBFhxodHRwczovL3d3dy5kaWdpY2Vy
dC5jb20vQ1BTMAgGBmeBDAECAjB8BggrBgEFBQcBAQRwMG4wJAYIKwYBBQUHMAGG
GGh0dHA6Ly9vY3NwLmRpZ2ljZXJ0LmNvbTBGBggrBgEFBQcwAoY6aHR0cDovL2Nh
Y2VydHMuZGlnaWNlcnQuY29tL0RpZ2lDZXJ0U0hBMlNlY3VyZVNlcnZlckNBLmNy
dDAMBgNVHRMBAf8EAjAAMIIBAwYKKwYBBAHWeQIEAgSB9ASB8QDvAHYA7ku9t3XO
YLrhQmkfq+GeZqMPfl+wctiDAMR7iXqo/csAAAFnVnMTfQAABAMARzBFAiBJgrib
BQubP427vWAfp3FM0/4FL11jj9hiB7iz53dXDgIhAOhRgg/LvdoLucTwv1JTbvIg
5QfQwVvsQmWWdjEIssBKAHUAh3W/51l8+IxDmV+9827/Vo1HVjb/SrVgwbTq/16g
gw8AAAFnVnMUVQAABAMARjBEAiB+WVXWhZs8hJQV26PQoIIP2YRlYLXl5xJzVVKg
kNZytQIgANctiBeHIJ71PkO8q+jRFALfTs17ftOugVf90qY+qu4wDQYJKoZIhvcN
AQELBQADggEBAJ20jQMNqcw4ZfCJNNVK/wyIZrEelqRvhhUibJDlmWApYU/Yfket
dwXY9/bmuHTHIuhzzvAaWNDYOnBMnjBENFjGzdpLOkN/u7v3HMRBs8JskHdz3Ghk
vxmI1aGgLKxM/Y2ZjdMO33ISzZZclTr6edY6E4GNvtiYZDArYRfVkgTXM+dR53Se
6FfhT1x96OaqYzn9u2PZ01sBZnCQLwmXNlLpa/tsS3YqY6x6YDByqE8xNV/GPPFX
eZBPwcK/4FJKMoyA01zsSNH+7DtRSwhbS2FQvNw8jVGh8SUPv8MtGPmVDwHgVEm6
O/YdUH9g3xVN+6IWMfA2Nm5jXhr9cdEK5zY=  
-----END CERTIFICATE-----  `,
	},
}

func TestDecodeCertificate(t *testing.T) {
	for i, test := range decodeCertificatesTests {
		filename, err := createTempFile(test.FileName, test.Certificate)
		if err != nil {
			t.Fatalf("Test %d: failed to write certificate: %s", i, err)
		}
		defer os.Remove(filename)
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatalf("Test %d: failed to read certificate: %s", i, err)
		}
		if _, err = decodeCertificates(data); err != nil {
			t.Fatalf("Test %d: failed to decode certificate: %s", i, err)
		}
	}
}
func TestGetRootCAs(t *testing.T) {
	emptydir, err := ioutil.TempDir("", "test-get-root-cas")
	if err != nil {
		t.Fatalf("Unable create temp directory. %v", emptydir)
	}
	defer os.RemoveAll(emptydir)

	dir1, err := ioutil.TempDir("", "test-get-root-cas")
	if err != nil {
		t.Fatalf("Unable create temp directory. %v", dir1)
	}
	defer os.RemoveAll(dir1)
	if err = os.Mkdir(filepath.Join(dir1, "empty-dir"), 0755); err != nil {
		t.Fatalf("Unable create empty dir. %v", err)
	}

	dir2, err := ioutil.TempDir("", "test-get-root-cas")
	if err != nil {
		t.Fatalf("Unable create temp directory. %v", dir2)
	}
	defer os.RemoveAll(dir2)
	if err = ioutil.WriteFile(filepath.Join(dir2, "empty-file"), []byte{}, 0644); err != nil {
		t.Fatalf("Unable create test file. %v", err)
	}

	testCases := []struct {
		certCAsDir  string
		expectedErr error
	}{
		// ignores non-existent directories.
		{"nonexistent-dir", nil},
		// Ignores directories.
		{dir1, nil},
		// Ignore empty directory.
		{emptydir, nil},
		// Loads the cert properly.
		{dir2, nil},
	}

	for _, testCase := range testCases {
		_, err := getRootCAs(testCase.certCAsDir)

		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}

func TestLoadX509KeyPair(t *testing.T) {
	for i, testCase := range loadX509KeyPairTests {
		privateKey, err := createTempFile("private.key", testCase.privateKey)
		if err != nil {
			t.Fatalf("Test %d: failed to create tmp private key file: %v", i, err)
		}
		certificate, err := createTempFile("public.crt", testCase.certificate)
		if err != nil {
			os.Remove(privateKey)
			t.Fatalf("Test %d: failed to create tmp certificate file: %v", i, err)
		}

		os.Unsetenv(TLSPrivateKeyPassword)
		if testCase.password != "" {
			os.Setenv(TLSPrivateKeyPassword, testCase.password)
		}
		_, _, err = loadX509KeyPair(certificate, privateKey)
		if err != nil && !testCase.shouldFail {
			t.Errorf("Test %d: test should succeed but it failed: %v", i, err)
		}
		if err == nil && testCase.shouldFail {
			t.Errorf("Test %d: test should fail but it succeed", i)
		}
		os.Remove(privateKey)
		os.Remove(certificate)
	}
}

var loadX509KeyPairTests = []struct {
	password                string
	privateKey, certificate string
	shouldFail              bool
}{
	{
		password: "foobar",
		privateKey: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-128-CBC,CC483BF11678C35F9F02A1AD85DAE285

nMDFd+Qxk1f+S7LwMitmMofNXYNbCY4L1QEqPOOx5wnjNF1wSxmEkL7+h8W4Y/vb
AQt/7TCcUSuSqEMl45nUIcCbhBos5wz+ShvFiez3qKwmR5HSURvqyN6PIJeAbU+h
uw/cvAQsCH1Cq+gYkDJqjrizPhGqg7mSkqyeST3PbOl+ZXc0wynIjA34JSwO3c5j
cF7XKHETtNGj1+AiLruX4wYZAJwQnK375fCoNVMO992zC6K83d8kvGMUgmJjkiIj
q3s4ymFGfoo0S/XNDQXgE5A5QjAKRKUyW2i7pHIIhTyOpeJQeFHDi2/zaZRxoCog
lD2/HKLi5xJtRelZaaGyEJ20c05VzaSZ+EtRIN33foNdyQQL6iAUU3hJ6JlcmRIB
bRfX4XPH1w9UfFU5ZKwUciCoDcL65bsyv/y56ItljBp7Ok+UUKl0H4myFNOSfsuU
IIj4neslnAvwQ8SN4XUpug+7pGF+2m/5UDwRzSUN1H2RfgWN95kqR+tYqCq/E+KO
i0svzFrljSHswsFoPBqKngI7hHwc9QTt5q4frXwj9I4F6HHrTKZnC5M4ef26sbJ1
r7JRmkt0h/GfcS355b0uoBTtF1R8tSJo85Zh47wE+ucdjEvy9/pjnzKqIoJo9bNZ
ri+ue7GhH5EUca1Kd10bH8FqTF+8AHh4yW6xMxSkSgFGp7KtraAVpdp+6kosymqh
dz9VMjA8i28btfkS2isRaCpyumaFYJ3DJMFYhmeyt6gqYovmRLX0qrBf8nrkFTAA
ZmykWsc8ErsCudxlDmKVemuyFL7jtm9IRPq+Jh+IrmixLJFx8PKkNAM6g+A8irx8
piw+yhRsVy5Jk2QeIqvbpxN6BfCNcix4sWkusiCJrAqQFuSm26Mhh53Ig1DXG4d3
6QY1T8tW80Q6JHUtDR+iOPqW6EmrNiEopzirvhGv9FicXZ0Lo2yKJueeeihWhFLL
GmlnCjWVMO4hoo8lWCHv95JkPxGMcecCacKKUbHlXzCGyw3+eeTEHMWMEhziLeBy
HZJ1/GReI3Sx7XlUCkG4468Yz3PpmbNIk/U5XKE7TGuxKmfcWQpu022iF/9DrKTz
KVhKimCBXJX345bCFe1rN2z5CV6sv87FkMs5Y+OjPw6qYFZPVKO2TdUUBcpXbQMg
UW+Kuaax9W7214Stlil727MjRCiH1+0yODg4nWj4pTSocA5R3pn5cwqrjMu97OmL
ESx4DHmy4keeSy3+AIAehCZlwgeLb70/xCSRhJMIMS9Q6bz8CPkEWN8bBZt95oeo
37LqZ7lNmq61fs1x1tq0VUnI9HwLFEnsiubp6RG0Yu8l/uImjjjXa/ytW2GXrfUi
zM22dOntu6u23iBxRBJRWdFTVUz7qrdu+PHavr+Y7TbCeiBwiypmz5llf823UIVx
btamI6ziAq2gKZhObIhut7sjaLkAyTLlNVkNN1WNaplAXpW25UFVk93MHbvZ27bx
9iLGs/qB2kDTUjffSQoHTLY1GoLxv83RgVspUGQjslztEEpWfYvGfVLcgYLv933B
aRW9BRoNZ0czKx7Lhuwjreyb5IcWDarhC8q29ZkkWsQQonaPb0kTEFJul80Yqk0k
-----END RSA PRIVATE KEY-----`,
		certificate: `-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIJAK5m5S7EE46kMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIxODE4
MDUyOFoXDTI3MTIxNjE4MDUyOFowWzELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0
YXRlMREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMRIw
EAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDPJfYY5Dhsntrqwyu7ZgKM/zrlKEjCwGHhWJBdZdeZCHQlY8ISrtDxxp2XMmI6
HsszalEhNF9fk3vSXWclTuomG03fgGzP4R6QpcwGUCxhRF1J+0b64Yi8pw2uEGsR
GuMwLhGorcWalNoihgHc0BQ4vO8aaTNTX7iD06olesP6vGNu/S8h0VomE+0v9qYc
VF66Zaiv/6OmxAtDpElJjVd0mY7G85BlDlFrVwzd7zhRiuJZ4iDg749Xt9GuuKla
Dvr14glHhP4dQgUbhluJmIHMdx2ZPjk+5FxaDK6I9IUpxczFDe4agDE6lKzU1eLd
cCXRWFOf6q9lTB1hUZfmWfTxAgMBAAGjUDBOMB0GA1UdDgQWBBTQh7lDTq+8salD
0HBNILochiiNaDAfBgNVHSMEGDAWgBTQh7lDTq+8salD0HBNILochiiNaDAMBgNV
HRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAqi9LycxcXKNSDXaPkCKvw7RQy
iMBDGm1kIY++p3tzbUGuaeu85TsswKnqd50AullEU+aQxRRJGfR8eSKzQJMBXLMQ
b4ptYCc5OrZtRHT8NaZ/df2tc6I88kN8dBu6ybcNGsevXA/iNX3kKLW7naxdr5jj
KUudWSuqDCjCmQa5bYb9H6DreLH2lUItSWBa/YmeZ3VSezDCd+XYO53QKwZVj8Jb
bulZmoo7e7HO1qecEzWKL10UYyEbG3UDPtw+NZc142ZYeEhXQ0dsstGAO5hf3hEl
kQyKGUTpDbKLuyYMFsoH73YLjBqNe+UEhPwE+FWpcky1Sp9RTx/oMLpiZaPR
-----END CERTIFICATE-----`,
		shouldFail: false,
	},
	{
		password: "password",
		privateKey: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-128-CBC,CC483BF11678C35F9F02A1AD85DAE285

nMDFd+Qxk1f+S7LwMitmMofNXYNbCY4L1QEqPOOx5wnjNF1wSxmEkL7+h8W4Y/vb
AQt/7TCcUSuSqEMl45nUIcCbhBos5wz+ShvFiez3qKwmR5HSURvqyN6PIJeAbU+h
uw/cvAQsCH1Cq+gYkDJqjrizPhGqg7mSkqyeST3PbOl+ZXc0wynIjA34JSwO3c5j
cF7XKHETtNGj1+AiLruX4wYZAJwQnK375fCoNVMO992zC6K83d8kvGMUgmJjkiIj
q3s4ymFGfoo0S/XNDQXgE5A5QjAKRKUyW2i7pHIIhTyOpeJQeFHDi2/zaZRxoCog
lD2/HKLi5xJtRelZaaGyEJ20c05VzaSZ+EtRIN33foNdyQQL6iAUU3hJ6JlcmRIB
bRfX4XPH1w9UfFU5ZKwUciCoDcL65bsyv/y56ItljBp7Ok+UUKl0H4myFNOSfsuU
IIj4neslnAvwQ8SN4XUpug+7pGF+2m/5UDwRzSUN1H2RfgWN95kqR+tYqCq/E+KO
i0svzFrljSHswsFoPBqKngI7hHwc9QTt5q4frXwj9I4F6HHrTKZnC5M4ef26sbJ1
r7JRmkt0h/GfcS355b0uoBTtF1R8tSJo85Zh47wE+ucdjEvy9/pjnzKqIoJo9bNZ
ri+ue7GhH5EUca1Kd10bH8FqTF+8AHh4yW6xMxSkSgFGp7KtraAVpdp+6kosymqh
dz9VMjA8i28btfkS2isRaCpyumaFYJ3DJMFYhmeyt6gqYovmRLX0qrBf8nrkFTAA
ZmykWsc8ErsCudxlDmKVemuyFL7jtm9IRPq+Jh+IrmixLJFx8PKkNAM6g+A8irx8
piw+yhRsVy5Jk2QeIqvbpxN6BfCNcix4sWkusiCJrAqQFuSm26Mhh53Ig1DXG4d3
6QY1T8tW80Q6JHUtDR+iOPqW6EmrNiEopzirvhGv9FicXZ0Lo2yKJueeeihWhFLL
GmlnCjWVMO4hoo8lWCHv95JkPxGMcecCacKKUbHlXzCGyw3+eeTEHMWMEhziLeBy
HZJ1/GReI3Sx7XlUCkG4468Yz3PpmbNIk/U5XKE7TGuxKmfcWQpu022iF/9DrKTz
KVhKimCBXJX345bCFe1rN2z5CV6sv87FkMs5Y+OjPw6qYFZPVKO2TdUUBcpXbQMg
UW+Kuaax9W7214Stlil727MjRCiH1+0yODg4nWj4pTSocA5R3pn5cwqrjMu97OmL
ESx4DHmy4keeSy3+AIAehCZlwgeLb70/xCSRhJMIMS9Q6bz8CPkEWN8bBZt95oeo
37LqZ7lNmq61fs1x1tq0VUnI9HwLFEnsiubp6RG0Yu8l/uImjjjXa/ytW2GXrfUi
zM22dOntu6u23iBxRBJRWdFTVUz7qrdu+PHavr+Y7TbCeiBwiypmz5llf823UIVx
btamI6ziAq2gKZhObIhut7sjaLkAyTLlNVkNN1WNaplAXpW25UFVk93MHbvZ27bx
9iLGs/qB2kDTUjffSQoHTLY1GoLxv83RgVspUGQjslztEEpWfYvGfVLcgYLv933B
aRW9BRoNZ0czKx7Lhuwjreyb5IcWDarhC8q29ZkkWsQQonaPb0kTEFJul80Yqk0k
-----END RSA PRIVATE KEY-----`,
		certificate: `-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIJAK5m5S7EE46kMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIxODE4
MDUyOFoXDTI3MTIxNjE4MDUyOFowWzELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0
YXRlMREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMRIw
EAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDPJfYY5Dhsntrqwyu7ZgKM/zrlKEjCwGHhWJBdZdeZCHQlY8ISrtDxxp2XMmI6
HsszalEhNF9fk3vSXWclTuomG03fgGzP4R6QpcwGUCxhRF1J+0b64Yi8pw2uEGsR
GuMwLhGorcWalNoihgHc0BQ4vO8aaTNTX7iD06olesP6vGNu/S8h0VomE+0v9qYc
VF66Zaiv/6OmxAtDpElJjVd0mY7G85BlDlFrVwzd7zhRiuJZ4iDg749Xt9GuuKla
Dvr14glHhP4dQgUbhluJmIHMdx2ZPjk+5FxaDK6I9IUpxczFDe4agDE6lKzU1eLd
cCXRWFOf6q9lTB1hUZfmWfTxAgMBAAGjUDBOMB0GA1UdDgQWBBTQh7lDTq+8salD
0HBNILochiiNaDAfBgNVHSMEGDAWgBTQh7lDTq+8salD0HBNILochiiNaDAMBgNV
HRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAqi9LycxcXKNSDXaPkCKvw7RQy
iMBDGm1kIY++p3tzbUGuaeu85TsswKnqd50AullEU+aQxRRJGfR8eSKzQJMBXLMQ
b4ptYCc5OrZtRHT8NaZ/df2tc6I88kN8dBu6ybcNGsevXA/iNX3kKLW7naxdr5jj
KUudWSuqDCjCmQa5bYb9H6DreLH2lUItSWBa/YmeZ3VSezDCd+XYO53QKwZVj8Jb
bulZmoo7e7HO1qecEzWKL10UYyEbG3UDPtw+NZc142ZYeEhXQ0dsstGAO5hf3hEl
kQyKGUTpDbKLuyYMFsoH73YLjBqNe+UEhPwE+FWpcky1Sp9RTx/oMLpiZaPR
-----END CERTIFICATE-----`,
		shouldFail: true,
	},
	{
		password: "",
		privateKey: `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA4K9Qq7vMY2bGkrdFAYpBYNLlCgnnFU+0pi+N+3bjuWmfX/kw
WXBa3SDqKD08PWWzwvBSLPCCUV2IuUd7tBa1pJ2wXkdoDeI5InYHJKrXbSZonni6
Bex7sgnqV/9o8xFkSOleoQWZgyeKGxtt0J/Z+zhpH+zXahwM4wOL3yzLSQt+NCKM
6N96zXYi16DEa89fYwRxPwE1XTRc7Ddggqx+4iRHvYG0fyTNcPB/+UiFw59EE1Sg
QIyTVntVqpsb6s8XdkFxURoLxefhcMVf2kU0T04OWI3gmeavKfTcj8Z2/bjPSsqP
mgkADv9Ru6VnSK/96TW/NwxWJ32PBz6Sbl9LdwIDAQABAoIBABVh+d5uH/RxyoIZ
+PI9kx1A1NVQvfI0RK/wJKYC2YdCuw0qLOTGIY+b20z7DumU7TenIVrvhKdzrFhd
qjMoWh8RdsByMT/pAKD79JATxi64EgrK2IFJ0TfPY8L+JqHDTPT3aK8QVly5/ZW4
1YmePOOAqdiE9Lc/diaApuYVYD9SL/X7fYs1ezOB4oGXoz0rthX77zHMxcEurpK3
VgSnaq7FYTVY7GrFB+ASiAlDIyLwztz08Ijn8aG0QAZ8GFuPGSmPMXWjLwFhRZsa
Gfy5BYiA0bVSnQSPHzAnHu9HyGlsdouVPPvJB3SrvMl+BFhZiUuR8OGSob7z7hfI
hMyHbNECgYEA/gyG7sHAb5mPkhq9JkTv+LrMY5NDZKYcSlbvBlM3kd6Ib3Hxl+6T
FMq2TWIrh2+mT1C14htziHd05dF6St995Tby6CJxTj6a/2Odnfm+JcOou/ula4Sz
92nIGlGPTJXstDbHGnRCpk6AomXK02stydTyrCisOw1H+LyTG6aT0q8CgYEA4mkO
hfLJkgmJzWIhxHR901uWHz/LId0gC6FQCeaqWmRup6Bl97f0U6xokw4tw8DJOncF
yZpYRXUXhdv/FXCjtXvAhKIX5+e+3dlzPHIdekSfcY00ip/ifAS1OyVviJia+cna
eJgq8WLHxJZim9Ah93NlPyiqGPwtasub90qjZbkCgYEA35WK02o1wII3dvCNc7bM
M+3CoAglEdmXoF1uM/TdPUXKcbqoU3ymeXAGjYhOov3CMp/n0z0xqvLnMLPxmx+i
ny6DDYXyjlhO9WFogHYhwP636+mHJl8+PAsfDvqk0VRJZDmpdUDIv7DrSQGpRfRX
8f+2K4oIOlhv9RuRpI4wHwUCgYB8OjaMyn1NEsy4k2qBt4U+jhcdyEv1pbWqi/U1
qYm5FTgd44VvWVDHBGdQoMv9h28iFCJpzrU2Txv8B4y7v9Ujg+ZLIAFL7j0szt5K
wTZpWvO9Q0Qb98Q2VgL2lADRiyIlglrMJnoRfiisNfOfGKE6e+eGsxI5qUxmN5e5
JQvoiQKBgQCqgyuUBIu/Qsb3qUED/o0S5wCel43Yh/Rl+mxDinOUvJfKJSW2SyEk
+jDo0xw3Opg6ZC5Lj2V809LA/XteaIuyhRuqOopjhHIvIvrYGe+2O8q9/Mv40BYW
0BhJ/Gdseps0C6Z5mTT5Fee4YVlGZuyuNKmKTd4JmqInfBV3ncMWQg==
-----END RSA PRIVATE KEY-----`,
		certificate: `-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIJAIb84Z5Mh31iMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIxODE4
NTcyM1oXDTI3MTIxNjE4NTcyM1owWzELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0
YXRlMREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMRIw
EAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDgr1Cru8xjZsaSt0UBikFg0uUKCecVT7SmL437duO5aZ9f+TBZcFrdIOooPTw9
ZbPC8FIs8IJRXYi5R3u0FrWknbBeR2gN4jkidgckqtdtJmieeLoF7HuyCepX/2jz
EWRI6V6hBZmDJ4obG23Qn9n7OGkf7NdqHAzjA4vfLMtJC340Iozo33rNdiLXoMRr
z19jBHE/ATVdNFzsN2CCrH7iJEe9gbR/JM1w8H/5SIXDn0QTVKBAjJNWe1Wqmxvq
zxd2QXFRGgvF5+FwxV/aRTRPTg5YjeCZ5q8p9NyPxnb9uM9Kyo+aCQAO/1G7pWdI
r/3pNb83DFYnfY8HPpJuX0t3AgMBAAGjUDBOMB0GA1UdDgQWBBQ2/bSCHscnoV+0
d+YJxLu4XLSNIDAfBgNVHSMEGDAWgBQ2/bSCHscnoV+0d+YJxLu4XLSNIDAMBgNV
HRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQC6p4gPwmkoDtRsP1c8IWgXFka+
Q59oe79ZK1RqDE6ZZu0rgw07rPzKr4ofW4hTxnx7PUgKOhWLq9VvwEC/9tDbD0Gw
SKknRZZOiEE3qUZbwNtHMd4UBzpzChTRC6RcwC5zT1/WICMUHxa4b8E2umJuf3Qd
5Y23sXEESx5evr49z6DLcVe2i70o2wJeWs2kaXqhCJt0X7z0rnYqjfFdvxd8dyzt
1DXmE45cLadpWHDg26DMsdchamgnqEo79YUxkH6G/Cb8ZX4igQ/CsxCDOKvccjHO
OncDtuIpK8O7OyfHP3+MBpUFG4P6Ctn7RVcZe9fQweTpfAy18G+loVzuUeOD
-----END CERTIFICATE-----`,
		shouldFail: false,
	},
	{
		password: "foobar",
		privateKey: `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA4K9Qq7vMY2bGkrdFAYpBYNLlCgnnFU+0pi+N+3bjuWmfX/kw
WXBa3SDqKD08PWWzwvBSLPCCUV2IuUd7tBa1pJ2wXkdoDeI5InYHJKrXbSZonni6
Bex7sgnqV/9o8xFkSOleoQWZgyeKGxtt0J/Z+zhpH+zXahwM4wOL3yzLSQt+NCKM
6N96zXYi16DEa89fYwRxPwE1XTRc7Ddggqx+4iRHvYG0fyTNcPB/+UiFw59EE1Sg
QIyTVntVqpsb6s8XdkFxURoLxefhcMVf2kU0T04OWI3gmeavKfTcj8Z2/bjPSsqP
mgkADv9Ru6VnSK/96TW/NwxWJ32PBz6Sbl9LdwIDAQABAoIBABVh+d5uH/RxyoIZ
+PI9kx1A1NVQvfI0RK/wJKYC2YdCuw0qLOTGIY+b20z7DumU7TenIVrvhKdzrFhd
qjMoWh8RdsByMT/pAKD79JATxi64EgrK2IFJ0TfPY8L+JqHDTPT3aK8QVly5/ZW4
1YmePOOAqdiE9Lc/diaApuYVYD9SL/X7fYs1ezOB4oGXoz0rthX77zHMxcEurpK3
VgSnaq7FYTVY7GrFB+ASiAlDIyLwztz08Ijn8aG0QAZ8GFuPGSmPMXWjLwFhRZsa
Gfy5BYiA0bVSnQSPHzAnHu9HyGlsdouVPPvJB3SrvMl+BFhZiUuR8OGSob7z7hfI
hMyHbNECgYEA/gyG7sHAb5mPkhq9JkTv+LrMY5NDZKYcSlbvBlM3kd6Ib3Hxl+6T
FMq2TWIrh2+mT1C14htziHd05dF6St995Tby6CJxTj6a/2Odnfm+JcOou/ula4Sz
92nIGlGPTJXstDbHGnRCpk6AomXK02stydTyrCisOw1H+LyTG6aT0q8CgYEA4mkO
hfLJkgmJzWIhxHR901uWHz/LId0gC6FQCeaqWmRup6Bl97f0U6xokw4tw8DJOncF
yZpYRXUXhdv/FXCjtXvAhKIX5+e+3dlzPHIdekSfcY00ip/ifAS1OyVviJia+cna
eJgq8WLHxJZim9Ah93NlPyiqGPwtasub90qjZbkCgYEA35WK02o1wII3dvCNc7bM
M+3CoAglEdmXoF1uM/TdPUXKcbqoU3ymeXAGjYhOov3CMp/n0z0xqvLnMLPxmx+i
ny6DDYXyjlhO9WFogHYhwP636+mHJl8+PAsfDvqk0VRJZDmpdUDIv7DrSQGpRfRX
8f+2K4oIOlhv9RuRpI4wHwUCgYB8OjaMyn1NEsy4k2qBt4U+jhcdyEv1pbWqi/U1
qYm5FTgd44VvWVDHBGdQoMv9h28iFCJpzrU2Txv8B4y7v9Ujg+ZLIAFL7j0szt5K
wTZpWvO9Q0Qb98Q2VgL2lADRiyIlglrMJnoRfiisNfOfGKE6e+eGsxI5qUxmN5e5
JQvoiQKBgQCqgyuUBIu/Qsb3qUED/o0S5wCel43Yh/Rl+mxDinOUvJfKJSW2SyEk
+jDo0xw3Opg6ZC5Lj2V809LA/XteaIuyhRuqOopjhHIvIvrYGe+2O8q9/Mv40BYW
0BhJ/Gdseps0C6Z5mTT5Fee4YVlGZuyuNKmKTd4JmqInfBV3ncMWQg==
-----END RSA PRIVATE KEY-----`,
		certificate: `-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIJAIb84Z5Mh31iMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIxODE4
NTcyM1oXDTI3MTIxNjE4NTcyM1owWzELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0
YXRlMREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMRIw
EAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDgr1Cru8xjZsaSt0UBikFg0uUKCecVT7SmL437duO5aZ9f+TBZcFrdIOooPTw9
ZbPC8FIs8IJRXYi5R3u0FrWknbBeR2gN4jkidgckqtdtJmieeLoF7HuyCepX/2jz
EWRI6V6hBZmDJ4obG23Qn9n7OGkf7NdqHAzjA4vfLMtJC340Iozo33rNdiLXoMRr
z19jBHE/ATVdNFzsN2CCrH7iJEe9gbR/JM1w8H/5SIXDn0QTVKBAjJNWe1Wqmxvq
zxd2QXFRGgvF5+FwxV/aRTRPTg5YjeCZ5q8p9NyPxnb9uM9Kyo+aCQAO/1G7pWdI
r/3pNb83DFYnfY8HPpJuX0t3AgMBAAGjUDBOMB0GA1UdDgQWBBQ2/bSCHscnoV+0
d+YJxLu4XLSNIDAfBgNVHSMEGDAWgBQ2/bSCHscnoV+0d+YJxLu4XLSNIDAMBgNV
HRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQC6p4gPwmkoDtRsP1c8IWgXFka+
Q59oe79ZK1RqDE6ZZu0rgw07rPzKr4ofW4hTxnx7PUgKOhWLq9VvwEC/9tDbD0Gw
SKknRZZOiEE3qUZbwNtHMd4UBzpzChTRC6RcwC5zT1/WICMUHxa4b8E2umJuf3Qd
5Y23sXEESx5evr49z6DLcVe2i70o2wJeWs2kaXqhCJt0X7z0rnYqjfFdvxd8dyzt
1DXmE45cLadpWHDg26DMsdchamgnqEo79YUxkH6G/Cb8ZX4igQ/CsxCDOKvccjHO
OncDtuIpK8O7OyfHP3+MBpUFG4P6Ctn7RVcZe9fQweTpfAy18G+loVzuUeOD
-----END CERTIFICATE-----`,
		shouldFail: false,
	},
}
