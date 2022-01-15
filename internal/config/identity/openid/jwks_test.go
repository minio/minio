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
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"testing"
)

func TestAzurePublicKey(t *testing.T) {
	const jsonkey = `{"keys":[{"kty":"RSA","use":"sig","kid":"SsZsBNhZcF3Q9S4trpQBTByNRRI","x5t":"SsZsBNhZcF3Q9S4trpQBTByNRRI","n":"uHPewhg4WC3eLVPkEFlj7RDtaKYWXCI5G-LPVzsMKOuIu7qQQbeytIA6P6HT9_iIRt8zNQvuw4P9vbNjgUCpI6vfZGsjk3XuCVoB_bAIhvuBcQh9ePH2yEwS5reR-NrG1PsqzobnZZuigKCoDmuOb_UDx1DiVyNCbMBlEG7UzTQwLf5NP6HaRHx027URJeZvPAWY7zjHlSOuKoS_d1yUveaBFIgZqPWLCg44ck4gvik45HsNVWT9zYfT74dvUSSrMSR-SHFT7Hy1XjbVXpHJHNNAXpPoGoWXTuc0BxMsB4cqjfJqoftFGOG4x32vEzakArLPxAKwGvkvu0jToAyvSQ","e":"AQAB","x5c":"MIIDBTCCAe2gAwIBAgIQWHw7h/Ysh6hPcXpnrJ0N8DANBgkqhkiG9w0BAQsFADAtMSswKQYDVQQDEyJhY2NvdW50cy5hY2Nlc3Njb250cm9sLndpbmRvd3MubmV0MB4XDTIwMDQyNzAwMDAwMFoXDTI1MDQyNzAwMDAwMFowLTErMCkGA1UEAxMiYWNjb3VudHMuYWNjZXNzY29udHJvbC53aW5kb3dzLm5ldDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALhz3sIYOFgt3i1T5BBZY+0Q7WimFlwiORviz1c7DCjriLu6kEG3srSAOj+h0/f4iEbfMzUL7sOD/b2zY4FAqSOr32RrI5N17glaAf2wCIb7gXEIfXjx9shMEua3kfjaxtT7Ks6G52WbooCgqA5rjm/1A8dQ4lcjQmzAZRBu1M00MC3+TT+h2kR8dNu1ESXmbzwFmO84x5UjriqEv3dclL3mgRSIGaj1iwoOOHJOIL4pOOR7DVVk/c2H0++Hb1EkqzEkfkhxU+x8tV421V6RyRzTQF6T6BqFl07nNAcTLAeHKo3yaqH7RRjhuMd9rxM2pAKyz8QCsBr5L7tI06AMr0kCAwEAAaMhMB8wHQYDVR0OBBYEFOI7M+DDFMlP7Ac3aomPnWo1QL1SMA0GCSqGSIb3DQEBCwUAA4IBAQBv+8rBiDY8sZDBoUDYwFQM74QjqCmgNQfv5B0Vjwg20HinERjQeH24uAWzyhWN9++FmeY4zcRXDY5UNmB0nJz7UGlprA9s7voQ0Lkyiud0DO072RPBg38LmmrqoBsLb3MB9MZ2CGBaHftUHfpdTvrgmXSP0IJn7mCUq27g+hFk7n/MLbN1k8JswEODIgdMRvGqN+mnrPKkviWmcVAZccsWfcmS1pKwXqICTKzd6WmVdz+cL7ZSd9I2X0pY4oRwauoE2bS95vrXljCYgLArI3XB2QcnglDDBRYu3Z3aIJb26PTIyhkVKT7xaXhXl4OgrbmQon9/O61G2dzpjzzBPqNP","issuer":"https://login.microsoftonline.com/906aefe9-76a7-4f65-b82d-5ec20775d5aa/v2.0"},{"kty":"RSA","use":"sig","kid":"huN95IvPfehq34GzBDZ1GXGirnM","x5t":"huN95IvPfehq34GzBDZ1GXGirnM","n":"6lldKm5Rc_vMKa1RM_TtUv3tmtj52wLRrJqu13yGM3_h0dwru2ZP53y65wDfz6_tLCjoYuRCuVsjoW37-0zXUORJvZ0L90CAX-58lW7NcE4bAzA1pXv7oR9kQw0X8dp0atU4HnHeaTU8LZxcjJO79_H9cxgwa-clKfGxllcos8TsuurM8xi2dx5VqwzqNMB2s62l3MTN7AzctHUiQCiX2iJArGjAhs-mxS1wmyMIyOSipdodhjQWRAcseW-aFVyRTFVi8okl2cT1HJjPXdx0b1WqYSOzeRdrrLUcA0oR2Tzp7xzOYJZSGNnNLQqa9f6h6h52XbX0iAgxKgEDlRpbJw","e":"AQAB","x5c":["MIIDBTCCAe2gAwIBAgIQPCxFbySVSLZOggeWRzBWOjANBgkqhkiG9w0BAQsFADAtMSswKQYDVQQDEyJhY2NvdW50cy5hY2Nlc3Njb250cm9sLndpbmRvd3MubmV0MB4XDTIwMDYwNzAwMDAwMFoXDTI1MDYwNzAwMDAwMFowLTErMCkGA1UEAxMiYWNjb3VudHMuYWNjZXNzY29udHJvbC53aW5kb3dzLm5ldDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOpZXSpuUXP7zCmtUTP07VL97ZrY+dsC0ayartd8hjN/4dHcK7tmT+d8uucA38+v7Swo6GLkQrlbI6Ft+/tM11DkSb2dC/dAgF/ufJVuzXBOGwMwNaV7+6EfZEMNF/HadGrVOB5x3mk1PC2cXIyTu/fx/XMYMGvnJSnxsZZXKLPE7LrqzPMYtnceVasM6jTAdrOtpdzEzewM3LR1IkAol9oiQKxowIbPpsUtcJsjCMjkoqXaHYY0FkQHLHlvmhVckUxVYvKJJdnE9RyYz13cdG9VqmEjs3kXa6y1HANKEdk86e8czmCWUhjZzS0KmvX+oeoedl219IgIMSoBA5UaWycCAwEAAaMhMB8wHQYDVR0OBBYEFFXP0ODFhjf3RS6oRijM5Tb+yB8CMA0GCSqGSIb3DQEBCwUAA4IBAQB9GtVikLTbJWIu5x9YCUTTKzNhi44XXogP/v8VylRSUHI5YTMdnWwvDIt/Y1sjNonmSy9PrioEjcIiI1U8nicveafMwIq5VLn+gEY2lg6KDJAzgAvA88CXqwfHHvtmYBovN7goolp8TY/kddMTf6TpNzN3lCTM2MK4Ye5xLLVGdp4bqWCOJ/qjwDxpTRSydYIkLUDwqNjv+sYfOElJpYAB4rTL/aw3ChJ1iaA4MtXEt6OjbUtbOa21lShfLzvNRbYK3+ukbrhmRl9lemJEeUls51vPuIe+jg+Ssp43aw7PQjxt4/MpfNMS2BfZ5F8GVSVG7qNb352cLLeJg5rc398Z"],"issuer":"https://login.microsoftonline.com/906aefe9-76a7-4f65-b82d-5ec20775d5aa/v2.0"},{"kty":"RSA","use":"sig","kid":"M6pX7RHoraLsprfJeRCjSxuURhc","x5t":"M6pX7RHoraLsprfJeRCjSxuURhc","n":"xHScZMPo8FifoDcrgncWQ7mGJtiKhrsho0-uFPXg-OdnRKYudTD7-Bq1MDjcqWRf3IfDVjFJixQS61M7wm9wALDj--lLuJJ9jDUAWTA3xWvQLbiBM-gqU0sj4mc2lWm6nPfqlyYeWtQcSC0sYkLlayNgX4noKDaXivhVOp7bwGXq77MRzeL4-9qrRYKjuzHfZL7kNBCsqO185P0NI2Jtmw-EsqYsrCaHsfNRGRrTvUHUq3hWa859kK_5uNd7TeY2ZEwKVD8ezCmSfR59ZzyxTtuPpkCSHS9OtUvS3mqTYit73qcvprjl3R8hpjXLb8oftfpWr3hFRdpxrwuoQEO4QQ","e":"AQAB","x5c":["MIIC8TCCAdmgAwIBAgIQfEWlTVc1uINEc9RBi6qHMjANBgkqhkiG9w0BAQsFADAjMSEwHwYDVQQDExhsb2dpbi5taWNyb3NvZnRvbmxpbmUudXMwHhcNMTgxMDE0MDAwMDAwWhcNMjAxMDE0MDAwMDAwWjAjMSEwHwYDVQQDExhsb2dpbi5taWNyb3NvZnRvbmxpbmUudXMwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDEdJxkw+jwWJ+gNyuCdxZDuYYm2IqGuyGjT64U9eD452dEpi51MPv4GrUwONypZF/ch8NWMUmLFBLrUzvCb3AAsOP76Uu4kn2MNQBZMDfFa9AtuIEz6CpTSyPiZzaVabqc9+qXJh5a1BxILSxiQuVrI2BfiegoNpeK+FU6ntvAZervsxHN4vj72qtFgqO7Md9kvuQ0EKyo7Xzk/Q0jYm2bD4SypiysJoex81EZGtO9QdSreFZrzn2Qr/m413tN5jZkTApUPx7MKZJ9Hn1nPLFO24+mQJIdL061S9LeapNiK3vepy+muOXdHyGmNctvyh+1+laveEVF2nGvC6hAQ7hBAgMBAAGjITAfMB0GA1UdDgQWBBQ5TKadw06O0cvXrQbXW0Nb3M3h/DANBgkqhkiG9w0BAQsFAAOCAQEAI48JaFtwOFcYS/3pfS5+7cINrafXAKTL+/+he4q+RMx4TCu/L1dl9zS5W1BeJNO2GUznfI+b5KndrxdlB6qJIDf6TRHh6EqfA18oJP5NOiKhU4pgkF2UMUw4kjxaZ5fQrSoD9omjfHAFNjradnHA7GOAoF4iotvXDWDBWx9K4XNZHWvD11Td66zTg5IaEQDIZ+f8WS6nn/98nAVMDtR9zW7Te5h9kGJGfe6WiHVaGRPpBvqC4iypGHjbRwANwofZvmp5wP08hY1CsnKY5tfP+E2k/iAQgKKa6QoxXToYvP7rsSkglak8N5g/+FJGnq4wP6cOzgZpjdPMwaVt5432GA=="],"issuer":"https://login.microsoftonline.com/906aefe9-76a7-4f65-b82d-5ec20775d5aa/v2.0"}]}`

	var jk JWKS
	if err := json.Unmarshal([]byte(jsonkey), &jk); err != nil {
		t.Fatal("Unmarshal: ", err)
	} else if len(jk.Keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(jk.Keys))
	}

	var kids []string
	for ii, jks := range jk.Keys {
		_, err := jks.DecodePublicKey()
		if err != nil {
			t.Fatalf("Failed to decode key %d: %v", ii, err)
		}
		kids = append(kids, jks.Kid)
	}
	if len(kids) != 3 {
		t.Fatalf("Failed to find the expected number of kids: 3, got %d", len(kids))
	}
}

// A.1 - Example public keys
func TestPublicKey(t *testing.T) {
	const jsonkey = `{"keys":
       [
         {"kty":"EC",
          "crv":"P-256",
          "x":"MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4",
          "y":"4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM",
          "use":"enc",
          "kid":"1"},

         {"kty":"RSA",
          "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
          "e":"AQAB",
          "alg":"RS256",
          "kid":"2011-04-29"}
       ]
     }`

	var jk JWKS
	if err := json.Unmarshal([]byte(jsonkey), &jk); err != nil {
		t.Fatal("Unmarshal: ", err)
	} else if len(jk.Keys) != 2 {
		t.Fatalf("Expected 2 keys, got %d", len(jk.Keys))
	}

	keys := make([]crypto.PublicKey, len(jk.Keys))
	for ii, jks := range jk.Keys {
		var err error
		keys[ii], err = jks.DecodePublicKey()
		if err != nil {
			t.Fatalf("Failed to decode key %d: %v", ii, err)
		}
	}

	//nolint:gocritic
	if key0, ok := keys[0].(*ecdsa.PublicKey); !ok {
		t.Fatalf("Expected ECDSA key[0], got %T", keys[0])
	} else if key1, ok := keys[1].(*rsa.PublicKey); !ok {
		t.Fatalf("Expected RSA key[1], got %T", keys[1])
	} else if key0.Curve != elliptic.P256() {
		t.Fatal("Key[0] is not using P-256 curve")
	} else if !bytes.Equal(key0.X.Bytes(), []byte{
		0x30, 0xa0, 0x42, 0x4c, 0xd2,
		0x1c, 0x29, 0x44, 0x83, 0x8a, 0x2d, 0x75, 0xc9, 0x2b, 0x37, 0xe7, 0x6e, 0xa2,
		0xd, 0x9f, 0x0, 0x89, 0x3a, 0x3b, 0x4e, 0xee, 0x8a, 0x3c, 0xa, 0xaf, 0xec, 0x3e,
	}) {
		t.Fatalf("Bad key[0].X, got %v", key0.X.Bytes())
	} else if !bytes.Equal(key0.Y.Bytes(), []byte{
		0xe0, 0x4b, 0x65, 0xe9, 0x24,
		0x56, 0xd9, 0x88, 0x8b, 0x52, 0xb3, 0x79, 0xbd, 0xfb, 0xd5, 0x1e, 0xe8,
		0x69, 0xef, 0x1f, 0xf, 0xc6, 0x5b, 0x66, 0x59, 0x69, 0x5b, 0x6c, 0xce,
		0x8, 0x17, 0x23,
	}) {
		t.Fatalf("Bad key[0].Y, got %v", key0.Y.Bytes())
	} else if key1.E != 0x10001 {
		t.Fatalf("Bad key[1].E: %d", key1.E)
	} else if !bytes.Equal(key1.N.Bytes(), []byte{
		0xd2, 0xfc, 0x7b, 0x6a, 0xa, 0x1e,
		0x6c, 0x67, 0x10, 0x4a, 0xeb, 0x8f, 0x88, 0xb2, 0x57, 0x66, 0x9b, 0x4d, 0xf6,
		0x79, 0xdd, 0xad, 0x9, 0x9b, 0x5c, 0x4a, 0x6c, 0xd9, 0xa8, 0x80, 0x15, 0xb5,
		0xa1, 0x33, 0xbf, 0xb, 0x85, 0x6c, 0x78, 0x71, 0xb6, 0xdf, 0x0, 0xb, 0x55,
		0x4f, 0xce, 0xb3, 0xc2, 0xed, 0x51, 0x2b, 0xb6, 0x8f, 0x14, 0x5c, 0x6e, 0x84,
		0x34, 0x75, 0x2f, 0xab, 0x52, 0xa1, 0xcf, 0xc1, 0x24, 0x40, 0x8f, 0x79, 0xb5,
		0x8a, 0x45, 0x78, 0xc1, 0x64, 0x28, 0x85, 0x57, 0x89, 0xf7, 0xa2, 0x49, 0xe3,
		0x84, 0xcb, 0x2d, 0x9f, 0xae, 0x2d, 0x67, 0xfd, 0x96, 0xfb, 0x92, 0x6c, 0x19,
		0x8e, 0x7, 0x73, 0x99, 0xfd, 0xc8, 0x15, 0xc0, 0xaf, 0x9, 0x7d, 0xde, 0x5a,
		0xad, 0xef, 0xf4, 0x4d, 0xe7, 0xe, 0x82, 0x7f, 0x48, 0x78, 0x43, 0x24, 0x39,
		0xbf, 0xee, 0xb9, 0x60, 0x68, 0xd0, 0x47, 0x4f, 0xc5, 0xd, 0x6d, 0x90, 0xbf,
		0x3a, 0x98, 0xdf, 0xaf, 0x10, 0x40, 0xc8, 0x9c, 0x2, 0xd6, 0x92, 0xab, 0x3b,
		0x3c, 0x28, 0x96, 0x60, 0x9d, 0x86, 0xfd, 0x73, 0xb7, 0x74, 0xce, 0x7, 0x40,
		0x64, 0x7c, 0xee, 0xea, 0xa3, 0x10, 0xbd, 0x12, 0xf9, 0x85, 0xa8, 0xeb, 0x9f,
		0x59, 0xfd, 0xd4, 0x26, 0xce, 0xa5, 0xb2, 0x12, 0xf, 0x4f, 0x2a, 0x34, 0xbc,
		0xab, 0x76, 0x4b, 0x7e, 0x6c, 0x54, 0xd6, 0x84, 0x2, 0x38, 0xbc, 0xc4, 0x5, 0x87,
		0xa5, 0x9e, 0x66, 0xed, 0x1f, 0x33, 0x89, 0x45, 0x77, 0x63, 0x5c, 0x47, 0xa,
		0xf7, 0x5c, 0xf9, 0x2c, 0x20, 0xd1, 0xda, 0x43, 0xe1, 0xbf, 0xc4, 0x19, 0xe2,
		0x22, 0xa6, 0xf0, 0xd0, 0xbb, 0x35, 0x8c, 0x5e, 0x38, 0xf9, 0xcb, 0x5, 0xa, 0xea,
		0xfe, 0x90, 0x48, 0x14, 0xf1, 0xac, 0x1a, 0xa4, 0x9c, 0xca, 0x9e, 0xa0, 0xca, 0x83,
	}) {
		t.Fatalf("Bad key[1].N, got %v", key1.N.Bytes())
	}
}
