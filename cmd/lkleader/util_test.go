package lkleader

import (
	"testing"
	"time"
)

func Test_hashToUint(t *testing.T) {
	type args struct {
		s   string
		max int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"00", args{"", 150}, 61},
		{"01", args{"abc.com", 150}, 132},
		{"02", args{"bbc.com", 150}, 93},
		{"03", args{"bbc.com:8100", 150}, 46},
		{"04", args{"bbc1.com:8100", 150}, 51},
		{"05", args{"bbc2.com:8100", 150}, 26},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := hashToInt(tt.args.s, tt.args.max)
			if err != nil {
				t.Fatal(err)
			}
			equals(t, id, tt.want)
		})
	}
}

func Test_randomTimeout(t *testing.T) {
	// https://github.com/hashicorp/raft/blob/5f09c4ffdbcd2a53768e78c47717415de12b6728/util_test.go#L10
	start := time.Now()
	timeout := randomTimeout(time.Millisecond)

	select {
	case <-timeout:
		diff := time.Since(start)
		if diff < time.Millisecond {
			t.Fatalf("fired early")
		}
	case <-time.After(3 * time.Millisecond):
		t.Fatalf("timeout")
	}

	timeout = randomTimeout(0)
	equals(t, (<-chan time.Time)(nil), timeout)
}
