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
	"testing"

	xnet "github.com/minio/minio/pkg/net"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func TestNatsConnPlain(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = 14222
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{Name: "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true},
		Subject: "test",
	}
	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}

func TestNatsConnUserPass(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = 14223
	opts.Username = "testminio"
	opts.Password = "miniotest"
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{Name: "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true},
		Subject:  "test",
		Username: opts.Username,
		Password: opts.Password,
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}

func TestNatsConnToken(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = 14223
	opts.Authorization = "s3cr3t"
	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	clientConfig := &NATSArgs{
		Enable: true,
		Address: xnet.Host{Name: "localhost",
			Port:      (xnet.Port(opts.Port)),
			IsPortSet: true},
		Subject: "test",
		Token:   opts.Authorization,
	}

	con, err := clientConfig.connectNats()
	if err != nil {
		t.Errorf("Could not connect to nats: %v", err)
	}
	defer con.Close()
}
