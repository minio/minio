/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package ocsp

import (
	"errors"
	"testing"
)

func TestOCSPSelfSignedCerts(t *testing.T) {

	testCases := []struct {
		certBytes   []byte
		expectedErr error
	}{
		{[]byte{}, errors.New("no certificates were found while parsing the bundle")},
		{[]byte{0x34, 0x12}, errors.New("no certificates were found while parsing the bundle")},
		{[]byte(`-----BEGIN CERTIFICATE-----
MIIDgzCCAmugAwIBAgIJAM4AD8TWLRs1MA0GCSqGSIb3DQEBCwUAMFgxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjEPMA0GA1UEAwwGZG9tYWluMB4XDTE3MDUxNjExMzI0
MVoXDTI3MDUxNDExMzI0MVowWDELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0YXRl
MREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMQ8wDQYD
VQQDDAZkb21haW4wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDTpDyE
Fa00Ch5eAvSzV4eB2+G1gymWOM30wUJ0kgIHZZPmxPlNmUNU4aS/0IhfPoArLKs9
4VzXTkyXm16bybcSeIJpd1oAluR7gQd/R91IoKyv58Z3q48PgBRMBhyzUgK3Y5yu
iD5DqFJixYB158FcoXTJsJR/+6PXnyC62NWjLzz1sunyYlYCJCXdpoq33PW1H6hG
+huEgKcDLs6Iz/MZWvLF8+SpFsUolMDAcM/GLGpytvFcHcDn3RqUWO982SyJWeXx
qCcz9zogZFyGSVfSRZ6l9XrnrheH9qIoaKMI/wRwn57qQ4kCS1+t4WVOfdJSOlZD
opWdh5hdr3zi+zMlAgMBAAGjUDBOMB0GA1UdDgQWBBRsKdbtfHKWs/zludBpLbof
jkRdJzAfBgNVHSMEGDAWgBRsKdbtfHKWs/zludBpLbofjkRdJzAMBgNVHRMEBTAD
AQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCLqPW71pjkpBREhcimMNOT7jbe6rsMgFE+
YJ8WAU4NNYAbfzM/LtJG4nWkxo4wj/r39Y+x/uQ3NwA2TwnqZOrLo6Z4MSNbNG5h
zMJqPZr+HptFaU2BhtF1alsjfhUfOEE3C8t0DN/AQHsgJJMGv41/oeY54g5qNHtJ
vOsfBqDTQrCjIjU2R4laAkSS93DMEO36vRh97EYyzkl0YCeHccIP7wlpAGP7YJAm
WKA9K/eRMDh1jwd4WmG6cYX1FMz9eryTELZPrMoF0t4VhQ8icsgJ71SfReS5iRuh
yUeZjSIGfTIMU8Dv4HvECECdfl6S5g2QFi400hCwm3hYX9nBLMXU
-----END CERTIFICATE-----`), errors.New("no OCSP server specified in cert")},
	}

	for i, testCase := range testCases {
		_, _, err := GetOCSPForPEM(testCase.certBytes)
		if testCase.expectedErr == nil && err != nil {
			t.Errorf("Test %d, expected to succeed but failed with err = `%s`", i+1, err.Error())
		}
		if testCase.expectedErr != nil && err == nil {
			t.Errorf("Test %d, expected to fail but succeeded.", i+1)
		}
	}
}
