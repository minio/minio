/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/net"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/nsqio/go-nsq"
)

func TestNewNSQTarget(t *testing.T) {
	type args struct {
		id   string
		args NSQArgs
	}
	tests := []struct {
		name    string
		args    args
		want    *NSQTarget
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				id: "id",
				args: NSQArgs{
					Enable: true,
					Topic:  "",
					TLS: struct {
						Enable     bool `json:"enable"`
						SkipVerify bool `json:"skipVerify"`
					}{true, true},
				},
			},
			want: &NSQTarget{
				id: event.TargetID{ID: "id", Name: "nsq"},
				args: NSQArgs{
					Enable:      true,
					NSQDAddress: net.Host{},
					Topic:       "",
					TLS: struct {
						Enable     bool `json:"enable"`
						SkipVerify bool `json:"skipVerify"`
					}{true, true},
				},
				producer: &nsq.Producer{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewNSQTarget(tt.args.id, tt.args.args)
			// dirty hack, otherwhise cannot compare the pointers:
			tt.want.producer = got.producer
			if (err != nil) != tt.wantErr {
				t.Errorf("NewNSQTarget() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewNSQTarget() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
