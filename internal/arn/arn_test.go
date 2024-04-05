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

package arn

import (
	"reflect"
	"testing"
)

func TestARN_String(t *testing.T) {
	tests := []struct {
		arn  ARN
		want string
	}{
		{
			arn: ARN{
				Partition:    "minio",
				Service:      "iam",
				Region:       "us-east-1",
				ResourceType: "role",
				ResourceID:   "my-role",
			},
			want: "arn:minio:iam:us-east-1::role/my-role",
		},
		{
			arn: ARN{
				Partition:    "minio",
				Service:      "",
				Region:       "us-east-1",
				ResourceType: "role",
				ResourceID:   "my-role",
			},
			want: "arn:minio::us-east-1::role/my-role",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.arn.String(); got != tt.want {
				t.Errorf("ARN.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewIAMRoleARN(t *testing.T) {
	type args struct {
		resourceID   string
		serverRegion string
	}
	tests := []struct {
		name    string
		args    args
		want    ARN
		wantErr bool
	}{
		{
			name: "valid resource ID must succeed",
			args: args{
				resourceID:   "my-role",
				serverRegion: "us-east-1",
			},
			want: ARN{
				Partition:    "minio",
				Service:      "iam",
				Region:       "us-east-1",
				ResourceType: "role",
				ResourceID:   "my-role",
			},
			wantErr: false,
		},
		{
			name: "valid resource ID must succeed",
			args: args{
				resourceID:   "-my-role",
				serverRegion: "us-east-1",
			},
			want: ARN{
				Partition:    "minio",
				Service:      "iam",
				Region:       "us-east-1",
				ResourceType: "role",
				ResourceID:   "-my-role",
			},
			wantErr: false,
		},
		{
			name: "empty server region must succeed",
			args: args{
				resourceID:   "my-role",
				serverRegion: "",
			},
			want: ARN{
				Partition:    "minio",
				Service:      "iam",
				Region:       "",
				ResourceType: "role",
				ResourceID:   "my-role",
			},
			wantErr: false,
		},
		{
			name: "empty resource ID must fail",
			args: args{
				resourceID:   "",
				serverRegion: "us-east-1",
			},
			want:    ARN{},
			wantErr: true,
		},
		{
			name: "resource ID starting with '=' must fail",
			args: args{
				resourceID:   "=",
				serverRegion: "us-east-1",
			},
			want:    ARN{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIAMRoleARN(tt.args.resourceID, tt.args.serverRegion)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIAMRoleARN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewIAMRoleARN() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	type args struct {
		arnStr string
	}
	tests := []struct {
		name    string
		args    args
		wantArn ARN
		wantErr bool
	}{
		{
			name: "valid ARN must succeed",
			args: args{
				arnStr: "arn:minio:iam:us-east-1::role/my-role",
			},
			wantArn: ARN{
				Partition:    "minio",
				Service:      "iam",
				Region:       "us-east-1",
				ResourceType: "role",
				ResourceID:   "my-role",
			},
			wantErr: false,
		},
		{
			name: "valid ARN must succeed",
			args: args{
				arnStr: "arn:minio:iam:us-east-1::role/-my-role",
			},
			wantArn: ARN{
				Partition:    "minio",
				Service:      "iam",
				Region:       "us-east-1",
				ResourceType: "role",
				ResourceID:   "-my-role",
			},
			wantErr: false,
		},
		{
			name: "invalid ARN length must fail",
			args: args{
				arnStr: "arn:minio:",
			},
			wantArn: ARN{},
			wantErr: true,
		},
		{
			name: "invalid ARN partition must fail",
			args: args{
				arnStr: "arn:invalid:iam:us-east-1::role/my-role",
			},
			wantArn: ARN{},
			wantErr: true,
		},
		{
			name: "invalid ARN service must fail",
			args: args{
				arnStr: "arn:minio:invalid:us-east-1::role/my-role",
			},
			wantArn: ARN{},
			wantErr: true,
		},
		{
			name: "invalid ARN resource type must fail",
			args: args{
				arnStr: "arn:minio:iam:us-east-1::invalid",
			},
			wantArn: ARN{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotArn, err := Parse(tt.args.arnStr)
			if err == nil && tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if !reflect.DeepEqual(gotArn, tt.wantArn) {
					t.Errorf("Parse() gotArn = %v, want %v", gotArn, tt.wantArn)
				}
			}
		})
	}
}
