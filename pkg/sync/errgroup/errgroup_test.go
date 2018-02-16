/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
