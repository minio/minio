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

package dns

import (
	"path"
	"strings"

	"github.com/miekg/dns"
)

// msgPath converts a domainname to an etcd path. If s looks like service.staging.skydns.local.,
// the resulting key will be /skydns/local/skydns/staging/service .
func msgPath(s, prefix string) string {
	l := dns.SplitDomainName(s)
	for i, j := 0, len(l)-1; i < j; i, j = i+1, j-1 {
		l[i], l[j] = l[j], l[i]
	}
	return path.Join(append([]string{etcdPathSeparator + prefix + etcdPathSeparator}, l...)...)
}

// dnsJoin joins labels to form a fully qualified domain name. If the last label is
// the root label it is ignored. Not other syntax checks are performed.
func dnsJoin(labels ...string) string {
	if len(labels) == 0 {
		return ""
	}
	ll := len(labels)
	if labels[ll-1] == "." {
		return strings.Join(labels[:ll-1], ".") + "."
	}
	return dns.Fqdn(strings.Join(labels, "."))
}

// msgUnPath converts a etcd path to domainName.
func msgUnPath(s string) string {
	l := strings.Split(s, etcdPathSeparator)
	if l[len(l)-1] == "" {
		l = l[:len(l)-1]
	}
	if len(l) < 2 {
		return s
	}
	// start with 1, to strip /skydns
	for i, j := 1, len(l)-1; i < j; i, j = i+1, j-1 {
		l[i], l[j] = l[j], l[i]
	}
	return dnsJoin(l[1 : len(l)-1]...)
}
