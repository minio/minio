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

package errgroup

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGroupWithNErrs(t *testing.T) {
	err1 := fmt.Errorf("errgroup_test: 1")
	err2 := fmt.Errorf("errgroup_test: 2")

	cases := []struct {
		errs []error
	}{
		{errs: []error{nil}},
		{errs: []error{err1}},
		{errs: []error{err1, nil}},
		{errs: []error{err1, nil, err2}},
	}

	for j, tc := range cases {
		t.Run(fmt.Sprintf("Test%d", j+1), func(t *testing.T) {
			g := WithNErrs(len(tc.errs))
			for i, err := range tc.errs {
				err := err
				g.Go(func() error { return err }, i)
			}

			gotErrs := g.Wait()
			if !reflect.DeepEqual(gotErrs, tc.errs) {
				t.Errorf("Expected %#v, got %#v", tc.errs, gotErrs)
			}
		})
	}
}
