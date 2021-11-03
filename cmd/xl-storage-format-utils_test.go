package cmd

import (
	"testing"

	xhttp "github.com/minio/minio/internal/http"
)

func Test_hashDeterministicString(t *testing.T) {
	tests := []struct {
		name string
		arg  map[string]string
	}{
		{
			name: "zero",
			arg:  map[string]string{},
		},
		{
			name: "nil",
			arg:  nil,
		},
		{
			name: "one",
			arg:  map[string]string{"key": "value"},
		},
		{
			name: "several",
			arg: map[string]string{
				xhttp.AmzRestore:                 "FAILED",
				xhttp.ContentMD5:                 mustGetUUID(),
				xhttp.AmzBucketReplicationStatus: "PENDING",
				xhttp.ContentType:                "application/json",
			},
		},
		{
			name: "someempty",
			arg: map[string]string{
				xhttp.AmzRestore:                 "",
				xhttp.ContentMD5:                 mustGetUUID(),
				xhttp.AmzBucketReplicationStatus: "",
				xhttp.ContentType:                "application/json",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n = 100
			want := hashDeterministicString(tt.arg)
			m := tt.arg
			for i := 0; i < n; i++ {
				if got := hashDeterministicString(m); got != want {
					t.Errorf("hashDeterministicString() = %v, want %v", got, want)
				}
			}
			// Check casual collisions
			if m == nil {
				m = make(map[string]string)
			}
			m["12312312"] = ""
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}
			want = hashDeterministicString(m)
			delete(m, "12312312")
			m["another"] = ""

			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			want = hashDeterministicString(m)
			m["another"] = "hashDeterministicString"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			want = hashDeterministicString(m)
			m["another"] = "hashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicString"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

			// Flip key/value
			want = hashDeterministicString(m)
			delete(m, "another")
			m["hashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicStringhashDeterministicString"] = "another"
			if got := hashDeterministicString(m); got == want {
				t.Errorf("hashDeterministicString() = %v, does not want %v", got, want)
			}

		})
	}
}
