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

package config

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/http"
	"strings"

	color "github.com/minio/minio/pkg/color"
)

// Extra ASN1 OIDs that we may need to handle
var (
	oidEmailAddress = []int{1, 2, 840, 113549, 1, 9, 1}
)

// printName prints the fields of a distinguished name, which include such
// things as its common name and locality.
func printName(names []pkix.AttributeTypeAndValue, buf *strings.Builder) []string {
	values := []string{}
	for _, name := range names {
		oid := name.Type
		if len(oid) == 4 && oid[0] == 2 && oid[1] == 5 && oid[2] == 4 {
			switch oid[3] {
			case 3:
				values = append(values, fmt.Sprintf("CN=%s", name.Value))
			case 6:
				values = append(values, fmt.Sprintf("C=%s", name.Value))
			case 8:
				values = append(values, fmt.Sprintf("ST=%s", name.Value))
			case 10:
				values = append(values, fmt.Sprintf("O=%s", name.Value))
			case 11:
				values = append(values, fmt.Sprintf("OU=%s", name.Value))
			default:
				values = append(values, fmt.Sprintf("UnknownOID=%s", name.Type.String()))
			}
		} else if oid.Equal(oidEmailAddress) {
			values = append(values, fmt.Sprintf("emailAddress=%s", name.Value))
		} else {
			values = append(values, fmt.Sprintf("UnknownOID=%s", name.Type.String()))
		}
	}
	if len(values) > 0 {
		buf.WriteString(values[0])
		for i := 1; i < len(values); i++ {
			buf.WriteString(", " + values[i])
		}
		buf.WriteString("\n")
	}
	return values
}

// CertificateText returns a human-readable string representation
// of the certificate cert. The format is similar to the OpenSSL
// way of printing certificates (not identical).
func CertificateText(cert *x509.Certificate) string {
	var buf strings.Builder

	buf.WriteString(color.Blue("\nCertificate:\n"))
	if cert.SignatureAlgorithm != x509.UnknownSignatureAlgorithm {
		buf.WriteString(color.Blue("%4sSignature Algorithm: ", "") + color.Bold(fmt.Sprintf("%s\n", cert.SignatureAlgorithm)))
	}

	// Issuer information
	buf.WriteString(color.Blue("%4sIssuer: ", ""))
	printName(cert.Issuer.Names, &buf)

	// Validity information
	buf.WriteString(color.Blue("%4sValidity\n", ""))
	buf.WriteString(color.Bold(fmt.Sprintf("%8sNot Before: %s\n", "", cert.NotBefore.Format(http.TimeFormat))))
	buf.WriteString(color.Bold(fmt.Sprintf("%8sNot After : %s\n", "", cert.NotAfter.Format(http.TimeFormat))))

	return buf.String()
}
