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
 */

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
