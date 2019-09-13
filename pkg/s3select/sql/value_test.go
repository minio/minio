package sql

import (
	"fmt"
	"math"
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
	type fields struct {
		value interface{}
	}
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
