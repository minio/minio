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

package target

import (
	"path"
	"path/filepath"
	"testing"

	xnet "github.com/minio/minio/pkg/net"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func TestNatsConnTLSCustomCA(t *testing.T) {
	s, opts := natsserver.RunServerWithConfig(filepath.Join("testdata", "nats_tls.conf"))
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{Name: "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true},
		Subject:       "test",
		Secure:        true,
		CertAuthority: path.Join("testdata", "certs", "root_ca_cert.pem"),
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}

func TestNatsConnTLSClientAuthorization(t *testing.T) {
	s, opts := natsserver.RunServerWithConfig(filepath.Join("testdata", "nats_tls_client_cert.conf"))
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{Name: "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true},
		Subject:       "test",
		Secure:        true,
		CertAuthority: path.Join("testdata", "certs", "root_ca_cert.pem"),
		ClientCert:    path.Join("testdata", "certs", "nats_client_cert.pem"),
		ClientKey:     path.Join("testdata", "certs", "nats_client_key.pem"),
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}
