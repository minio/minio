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

package config

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/http"
	"strings"

	color "github.com/minio/minio/internal/color"
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
		//nolint:gocritic
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
