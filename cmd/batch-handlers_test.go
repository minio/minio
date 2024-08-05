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

package cmd

import (
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBatchJobPrefix_UnmarshalYAML(t *testing.T) {
	type args struct {
		yamlStr string
	}
	type PrefixTemp struct {
		Prefix BatchJobPrefix `yaml:"prefix"`
	}
	tests := []struct {
		name    string
		b       PrefixTemp
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "test1",
			b:    PrefixTemp{},
			args: args{
				yamlStr: `
prefix: "foo"
`,
			},
			want:    []string{"foo"},
			wantErr: false,
		},
		{
			name: "test2",
			b:    PrefixTemp{},
			args: args{
				yamlStr: `
prefix:
  - "foo"
  - "bar"
`,
			},
			want: []string{"foo", "bar"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := yaml.Unmarshal([]byte(tt.args.yamlStr), &tt.b); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalYAML() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !slices.Equal(tt.b.Prefix.F(), tt.want) {
				t.Errorf("UnmarshalYAML() = %v, want %v", tt.b.Prefix.F(), tt.want)
			}
		})
	}
}
