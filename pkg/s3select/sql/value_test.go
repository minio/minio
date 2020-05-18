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

package sql

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"
)

// valueBuilders contains one constructor for each value type.
// Values should match if type is the same.
var valueBuilders = []func() *Value{
	func() *Value {
		return FromNull()
	},
	func() *Value {
		return FromBool(true)
	},
	func() *Value {
		return FromBytes([]byte("byte contents"))
	},
	func() *Value {
		return FromFloat(math.Pi)
	},
	func() *Value {
		return FromInt(0x1337)
	},
	func() *Value {
		t, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
		if err != nil {
			panic(err)
		}
		return FromTimestamp(t)
	},
	func() *Value {
		return FromString("string contents")
	},
}

// altValueBuilders contains one constructor for each value type.
// Values are zero values and should NOT match the values in valueBuilders, except Null type.
var altValueBuilders = []func() *Value{
	func() *Value {
		return FromNull()
	},
	func() *Value {
		return FromBool(false)
	},
	func() *Value {
		return FromBytes(nil)
	},
	func() *Value {
		return FromFloat(0)
	},
	func() *Value {
		return FromInt(0)
	},
	func() *Value {
		return FromTimestamp(time.Time{})
	},
	func() *Value {
		return FromString("")
	},
}

func TestValue_SameTypeAs(t *testing.T) {
	type fields struct {
		a, b Value
	}
	type test struct {
		name   string
		fields fields
		wantOk bool
	}
	var tests []test
	for i := range valueBuilders {
		a := valueBuilders[i]()
		for j := range valueBuilders {
			b := valueBuilders[j]()
			tests = append(tests, test{
				name: fmt.Sprint(a.GetTypeString(), "==", b.GetTypeString()),
				fields: fields{
					a: *a, b: *b,
				},
				wantOk: i == j,
			})
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotOk := tt.fields.a.SameTypeAs(tt.fields.b); gotOk != tt.wantOk {
				t.Errorf("SameTypeAs() = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestValue_Equals(t *testing.T) {
	type fields struct {
		a, b Value
	}
	type test struct {
		name   string
		fields fields
		wantOk bool
	}
	var tests []test
	for i := range valueBuilders {
		a := valueBuilders[i]()
		for j := range valueBuilders {
			b := valueBuilders[j]()
			tests = append(tests, test{
				name: fmt.Sprint(a.GetTypeString(), "==", b.GetTypeString()),
				fields: fields{
					a: *a, b: *b,
				},
				wantOk: i == j,
			})
		}
	}
	for i := range valueBuilders {
		a := valueBuilders[i]()
		for j := range altValueBuilders {
			b := altValueBuilders[j]()
			tests = append(tests, test{
				name: fmt.Sprint(a.GetTypeString(), "!=", b.GetTypeString()),
				fields: fields{
					a: *a, b: *b,
				},
				// Only Null == Null
				wantOk: a.IsNull() && b.IsNull() && i == 0 && j == 0,
			})
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotOk := tt.fields.a.Equals(tt.fields.b); gotOk != tt.wantOk {
				t.Errorf("Equals() = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestValue_CSVString(t *testing.T) {
	type test struct {
		name    string
		want    string
		wantAlt string
	}

	tests := []test{
		{
			name:    valueBuilders[0]().String(),
			want:    "",
			wantAlt: "",
		},
		{
			name:    valueBuilders[1]().String(),
			want:    "true",
			wantAlt: "false",
		},
		{
			name:    valueBuilders[2]().String(),
			want:    "byte contents",
			wantAlt: "",
		},
		{
			name:    valueBuilders[3]().String(),
			want:    "3.141592653589793",
			wantAlt: "0",
		},
		{
			name:    valueBuilders[4]().String(),
			want:    "4919",
			wantAlt: "0",
		},
		{
			name:    valueBuilders[5]().String(),
			want:    "2006-01-02T15:04:05Z",
			wantAlt: "0001T",
		},
		{
			name:    valueBuilders[6]().String(),
			want:    "string contents",
			wantAlt: "",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := valueBuilders[i]()
			vAlt := altValueBuilders[i]()
			if got := v.CSVString(); got != tt.want {
				t.Errorf("CSVString() = %v, want %v", got, tt.want)
			}
			if got := vAlt.CSVString(); got != tt.wantAlt {
				t.Errorf("CSVString() = %v, want %v", got, tt.wantAlt)
			}
		})
	}
}

func TestValue_bytesToInt(t *testing.T) {
	type fields struct {
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
		wantOK bool
	}{
		{
			name: "zero",
			fields: fields{
				value: []byte("0"),
			},
			want:   0,
			wantOK: true,
		},
		{
			name: "minuszero",
			fields: fields{
				value: []byte("-0"),
			},
			want:   0,
			wantOK: true,
		},
		{
			name: "one",
			fields: fields{
				value: []byte("1"),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "minusone",
			fields: fields{
				value: []byte("-1"),
			},
			want:   -1,
			wantOK: true,
		},
		{
			name: "plusone",
			fields: fields{
				value: []byte("+1"),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "max",
			fields: fields{
				value: []byte(strconv.FormatInt(math.MaxInt64, 10)),
			},
			want:   math.MaxInt64,
			wantOK: true,
		},
		{
			name: "min",
			fields: fields{
				value: []byte(strconv.FormatInt(math.MinInt64, 10)),
			},
			want:   math.MinInt64,
			wantOK: true,
		},
		{
			name: "max-overflow",
			fields: fields{
				value: []byte("9223372036854775808"),
			},
			// Seems to be what strconv.ParseInt returns
			want:   math.MaxInt64,
			wantOK: false,
		},
		{
			name: "min-underflow",
			fields: fields{
				value: []byte("-9223372036854775809"),
			},
			// Seems to be what strconv.ParseInt returns
			want:   math.MinInt64,
			wantOK: false,
		},
		{
			name: "zerospace",
			fields: fields{
				value: []byte(" 0"),
			},
			want:   0,
			wantOK: true,
		},
		{
			name: "onespace",
			fields: fields{
				value: []byte("1 "),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "minusonespace",
			fields: fields{
				value: []byte(" -1 "),
			},
			want:   -1,
			wantOK: true,
		},
		{
			name: "plusonespace",
			fields: fields{
				value: []byte("\t+1\t"),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "scientific",
			fields: fields{
				value: []byte("3e5"),
			},
			want:   0,
			wantOK: false,
		},
		{
			// No support for prefixes
			name: "hex",
			fields: fields{
				value: []byte("0xff"),
			},
			want:   0,
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &Value{
				value: tt.fields.value,
			}
			got, got1 := v.bytesToInt()
			if got != tt.want {
				t.Errorf("bytesToInt() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.wantOK {
				t.Errorf("bytesToInt() got1 = %v, want %v", got1, tt.wantOK)
			}
		})
	}
}

func TestValue_bytesToFloat(t *testing.T) {
	type fields struct {
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
		wantOK bool
	}{
		// Copied from TestValue_bytesToInt.
		{
			name: "zero",
			fields: fields{
				value: []byte("0"),
			},
			want:   0,
			wantOK: true,
		},
		{
			name: "minuszero",
			fields: fields{
				value: []byte("-0"),
			},
			want:   0,
			wantOK: true,
		},
		{
			name: "one",
			fields: fields{
				value: []byte("1"),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "minusone",
			fields: fields{
				value: []byte("-1"),
			},
			want:   -1,
			wantOK: true,
		},
		{
			name: "plusone",
			fields: fields{
				value: []byte("+1"),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "maxint",
			fields: fields{
				value: []byte(strconv.FormatInt(math.MaxInt64, 10)),
			},
			want:   math.MaxInt64,
			wantOK: true,
		},
		{
			name: "minint",
			fields: fields{
				value: []byte(strconv.FormatInt(math.MinInt64, 10)),
			},
			want:   math.MinInt64,
			wantOK: true,
		},
		{
			name: "max-overflow-int",
			fields: fields{
				value: []byte("9223372036854775808"),
			},
			// Seems to be what strconv.ParseInt returns
			want:   math.MaxInt64,
			wantOK: true,
		},
		{
			name: "min-underflow-int",
			fields: fields{
				value: []byte("-9223372036854775809"),
			},
			// Seems to be what strconv.ParseInt returns
			want:   math.MinInt64,
			wantOK: true,
		},
		{
			name: "max",
			fields: fields{
				value: []byte(strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64)),
			},
			want:   math.MaxFloat64,
			wantOK: true,
		},
		{
			name: "min",
			fields: fields{
				value: []byte(strconv.FormatFloat(-math.MaxFloat64, 'g', -1, 64)),
			},
			want:   -math.MaxFloat64,
			wantOK: true,
		},
		{
			name: "max-overflow",
			fields: fields{
				value: []byte("1.797693134862315708145274237317043567981e+309"),
			},
			// Seems to be what strconv.ParseInt returns
			want:   math.Inf(1),
			wantOK: false,
		},
		{
			name: "min-underflow",
			fields: fields{
				value: []byte("-1.797693134862315708145274237317043567981e+309"),
			},
			// Seems to be what strconv.ParseInt returns
			want:   math.Inf(-1),
			wantOK: false,
		},
		{
			name: "smallest-pos",
			fields: fields{
				value: []byte(strconv.FormatFloat(math.SmallestNonzeroFloat64, 'g', -1, 64)),
			},
			want:   math.SmallestNonzeroFloat64,
			wantOK: true,
		},
		{
			name: "smallest-pos",
			fields: fields{
				value: []byte(strconv.FormatFloat(-math.SmallestNonzeroFloat64, 'g', -1, 64)),
			},
			want:   -math.SmallestNonzeroFloat64,
			wantOK: true,
		},
		{
			name: "zerospace",
			fields: fields{
				value: []byte(" 0"),
			},
			want:   0,
			wantOK: true,
		},
		{
			name: "onespace",
			fields: fields{
				value: []byte("1 "),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "minusonespace",
			fields: fields{
				value: []byte(" -1 "),
			},
			want:   -1,
			wantOK: true,
		},
		{
			name: "plusonespace",
			fields: fields{
				value: []byte("\t+1\t"),
			},
			want:   1,
			wantOK: true,
		},
		{
			name: "scientific",
			fields: fields{
				value: []byte("3e5"),
			},
			want:   300000,
			wantOK: true,
		},
		{
			// No support for prefixes
			name: "hex",
			fields: fields{
				value: []byte("0xff"),
			},
			want:   0,
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Value{
				value: tt.fields.value,
			}
			got, got1 := v.bytesToFloat()
			if got != tt.want {
				t.Errorf("bytesToFloat() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.wantOK {
				t.Errorf("bytesToFloat() got1 = %v, want %v", got1, tt.wantOK)
			}
		})
	}
}

func TestValue_bytesToBool(t *testing.T) {
	type fields struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		wantVal bool
		wantOk  bool
	}{
		{
			name: "true",
			fields: fields{
				value: []byte("true"),
			},
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "false",
			fields: fields{
				value: []byte("false"),
			},
			wantVal: false,
			wantOk:  true,
		},
		{
			name: "t",
			fields: fields{
				value: []byte("t"),
			},
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "f",
			fields: fields{
				value: []byte("f"),
			},
			wantVal: false,
			wantOk:  true,
		},
		{
			name: "1",
			fields: fields{
				value: []byte("1"),
			},
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "0",
			fields: fields{
				value: []byte("0"),
			},
			wantVal: false,
			wantOk:  true,
		},
		{
			name: "truespace",
			fields: fields{
				value: []byte(" true "),
			},
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "truetabs",
			fields: fields{
				value: []byte("\ttrue\t"),
			},
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "TRUE",
			fields: fields{
				value: []byte("TRUE"),
			},
			wantVal: true,
			wantOk:  true,
		},
		{
			name: "FALSE",
			fields: fields{
				value: []byte("FALSE"),
			},
			wantVal: false,
			wantOk:  true,
		},
		{
			name: "invalid",
			fields: fields{
				value: []byte("no"),
			},
			wantVal: false,
			wantOk:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Value{
				value: tt.fields.value,
			}
			gotVal, gotOk := v.bytesToBool()
			if gotVal != tt.wantVal {
				t.Errorf("bytesToBool() gotVal = %v, want %v", gotVal, tt.wantVal)
			}
			if gotOk != tt.wantOk {
				t.Errorf("bytesToBool() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
