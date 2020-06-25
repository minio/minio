/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package encoding

import (
	"math"
	"reflect"
	"testing"
)

func TestPlainEncodeBools(t *testing.T) {
	testCases := []struct {
		bs             []bool
		expectedResult []byte
	}{
		{nil, []byte{}},
		{[]bool{}, []byte{}},
		{[]bool{true}, []byte{1}},
		{[]bool{false}, []byte{0}},
		{[]bool{true, true}, []byte{3}},
		{[]bool{false, false}, []byte{0}},
		{[]bool{false, true}, []byte{2}},
		{[]bool{true, false}, []byte{1}},
		{[]bool{false, false, false, false, false, false, false, true, true}, []byte{128, 1}},
	}

	for i, testCase := range testCases {
		result := plainEncodeBools(testCase.bs)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPlainEncodeInt32s(t *testing.T) {
	testCases := []struct {
		i32s           []int32
		expectedResult []byte
	}{
		{nil, []byte{}},
		{[]int32{}, []byte{}},
		{[]int32{1}, []byte{1, 0, 0, 0}},
		{[]int32{-1}, []byte{255, 255, 255, 255}},
		{[]int32{256}, []byte{0, 1, 0, 0}},
		{[]int32{math.MinInt32}, []byte{0, 0, 0, 128}},
		{[]int32{math.MaxInt32}, []byte{255, 255, 255, 127}},
		{[]int32{257, -2}, []byte{1, 1, 0, 0, 254, 255, 255, 255}},
	}

	for i, testCase := range testCases {
		result := plainEncodeInt32s(testCase.i32s)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPlainEncodeInt64s(t *testing.T) {
	testCases := []struct {
		i64s           []int64
		expectedResult []byte
	}{
		{nil, []byte{}},
		{[]int64{}, []byte{}},
		{[]int64{1}, []byte{1, 0, 0, 0, 0, 0, 0, 0}},
		{[]int64{-1}, []byte{255, 255, 255, 255, 255, 255, 255, 255}},
		{[]int64{256}, []byte{0, 1, 0, 0, 0, 0, 0, 0}},
		{[]int64{math.MinInt64}, []byte{0, 0, 0, 0, 0, 0, 0, 128}},
		{[]int64{math.MaxInt64}, []byte{255, 255, 255, 255, 255, 255, 255, 127}},
		{[]int64{257, -2}, []byte{1, 1, 0, 0, 0, 0, 0, 0, 254, 255, 255, 255, 255, 255, 255, 255}},
	}

	for i, testCase := range testCases {
		result := plainEncodeInt64s(testCase.i64s)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPlainEncodeFloat32s(t *testing.T) {
	testCases := []struct {
		f32s           []float32
		expectedResult []byte
	}{
		{nil, []byte{}},
		{[]float32{}, []byte{}},
		{[]float32{1}, []byte{0, 0, 128, 63}},
		{[]float32{1.0}, []byte{0, 0, 128, 63}},
		{[]float32{-1}, []byte{0, 0, 128, 191}},
		{[]float32{-1.0}, []byte{0, 0, 128, 191}},
		{[]float32{256}, []byte{0, 0, 128, 67}},
		{[]float32{1.1}, []byte{205, 204, 140, 63}},
		{[]float32{-1.1}, []byte{205, 204, 140, 191}},
		{[]float32{math.Pi}, []byte{219, 15, 73, 64}},
		{[]float32{257, -2}, []byte{0, 128, 128, 67, 0, 0, 0, 192}},
		{[]float32{257.1, -2.1}, []byte{205, 140, 128, 67, 102, 102, 6, 192}},
	}

	for i, testCase := range testCases {
		result := plainEncodeFloat32s(testCase.f32s)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPlainEncodeFloat64s(t *testing.T) {
	testCases := []struct {
		f64s           []float64
		expectedResult []byte
	}{
		{nil, []byte{}},
		{[]float64{}, []byte{}},
		{[]float64{1}, []byte{0, 0, 0, 0, 0, 0, 240, 63}},
		{[]float64{1.0}, []byte{0, 0, 0, 0, 0, 0, 240, 63}},
		{[]float64{-1}, []byte{0, 0, 0, 0, 0, 0, 240, 191}},
		{[]float64{-1.0}, []byte{0, 0, 0, 0, 0, 0, 240, 191}},
		{[]float64{256}, []byte{0, 0, 0, 0, 0, 0, 112, 64}},
		{[]float64{1.1}, []byte{154, 153, 153, 153, 153, 153, 241, 63}},
		{[]float64{-1.1}, []byte{154, 153, 153, 153, 153, 153, 241, 191}},
		{[]float64{math.Pi}, []byte{24, 45, 68, 84, 251, 33, 9, 64}},
		{[]float64{257, -2}, []byte{0, 0, 0, 0, 0, 16, 112, 64, 0, 0, 0, 0, 0, 0, 0, 192}},
		{[]float64{257.1, -2.1}, []byte{154, 153, 153, 153, 153, 17, 112, 64, 205, 204, 204, 204, 204, 204, 0, 192}},
	}

	for i, testCase := range testCases {
		result := plainEncodeFloat64s(testCase.f64s)
		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
