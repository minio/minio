/*
 * MinIO Object Storage (c) 2021-2023 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package target

import (
	"path"
	"path/filepath"
	"testing"

	xnet "github.com/minio/pkg/v3/net"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func TestNatsConnTLSCustomCA(t *testing.T) {
	s, opts := natsserver.RunServerWithConfig(filepath.Join("testdata", "contrib", "nats_tls.conf"))
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{
			Name:      "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true,
		},
		Subject:       "test",
		Secure:        true,
		CertAuthority: path.Join("testdata", "contrib", "certs", "root_ca_cert.pem"),
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}

func TestNatsConnTLSCustomCAHandshakeFirst(t *testing.T) {
	s, opts := natsserver.RunServerWithConfig(filepath.Join("testdata", "contrib", "nats_tls_handshake_first.conf"))
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{
			Name:      "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true,
		},
		Subject:           "test",
		Secure:            true,
		CertAuthority:     path.Join("testdata", "contrib", "certs", "root_ca_cert.pem"),
		TLSHandshakeFirst: true,
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}

func TestNatsConnTLSClientAuthorization(t *testing.T) {
	s, opts := natsserver.RunServerWithConfig(filepath.Join("testdata", "contrib", "nats_tls_client_cert.conf"))
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{
			Name:      "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true,
		},
		Subject:       "test",
		Secure:        true,
		CertAuthority: path.Join("testdata", "contrib", "certs", "root_ca_cert.pem"),
		ClientCert:    path.Join("testdata", "contrib", "certs", "nats_client_cert.pem"),
		ClientKey:     path.Join("testdata", "contrib", "certs", "nats_client_key.pem"),
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}
