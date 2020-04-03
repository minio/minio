/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package s3

import (
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/bucket/object/tagging"
)

func TestGetTagMap(t *testing.T) {
	t2Map := map[string]string{
		"designation1": "value1",
		"designation2": "value2",
		"designation3": "value3",
		"designation4": "value4",
	}
	t3Map := map[string]string{
		"designation1": "",
		"designation2": "",
		"designation3": "",
		"designation4": "",
		"designation5": "",
	}

	testcases := []struct {
		tagStr string
		tagMap map[string]string
		err    error
	}{

		{
			tagStr: "designation1=value1&designation2=value2&designation3=value3&designation4=value&designation5=value5&designation6=value6&designation7=value7&designation8=value8&designation9=value9&designation10=value10&designation11=value11",
			tagMap: nil,
			err:    tagging.ErrTooManyTags,
		},
		{
			tagStr: "designation1=value1&designation2=value2&designation3=value3&designation4=value4",
			tagMap: t2Map,
			err:    nil,
		},
		{
			tagStr: "designation1&designation2=&designation3&designation4=&designation5",
			tagMap: t3Map,
			err:    nil,
		},
	}

	for i, tc := range testcases {
		tgmp, e := getTagMap(tc.tagStr)
		if (e != nil && tc.err != nil) && len(tgmp) != len(tc.tagMap) {
			t.Errorf("Test case %d: Expected map length %d but received length %d", i+1, len(tc.tagMap), len(tgmp))
		} else if e != tc.err {
			t.Errorf("Test case %d: Expected error %v but received error %v", i+1, tc.err, e)
		} else if eq := reflect.DeepEqual(tgmp, tc.tagMap); !eq {
			t.Errorf("Test case %d: Returned Tag maps are not equivalent", i+1)
		}
	}
}
