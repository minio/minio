// Copyright (c) 2015-2023 MinIO, Inc.
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

	xnet "github.com/minio/pkg/v3/net"
)

func TestNSQArgs_Validate(t *testing.T) {
	type fields struct {
		Enable      bool
		NSQDAddress xnet.Host
		Topic       string
		TLS         struct {
			Enable     bool
			SkipVerify bool
		}
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test1_missing_topic",
			fields: fields{
				Enable: true,
				NSQDAddress: xnet.Host{
					Name:      "127.0.0.1",
					Port:      4150,
					IsPortSet: true,
				},
				Topic: "",
			},
			wantErr: true,
		},
		{
			name: "test2_disabled",
			fields: fields{
				Enable:      false,
				NSQDAddress: xnet.Host{},
				Topic:       "topic",
			},
			wantErr: false,
		},
		{
			name: "test3_OK",
			fields: fields{
				Enable: true,
				NSQDAddress: xnet.Host{
					Name:      "127.0.0.1",
					Port:      4150,
					IsPortSet: true,
				},
				Topic: "topic",
			},
			wantErr: false,
		},
		{
			name: "test4_emptynsqdaddr",
			fields: fields{
				Enable:      true,
				NSQDAddress: xnet.Host{},
				Topic:       "topic",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NSQArgs{
				Enable:      tt.fields.Enable,
				NSQDAddress: tt.fields.NSQDAddress,
				Topic:       tt.fields.Topic,
			}
			if err := n.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("NSQArgs.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
