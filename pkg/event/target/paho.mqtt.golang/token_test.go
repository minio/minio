package mqtt

import (
	"testing"
	"time"
)

func TestWaitTimeout(t *testing.T) {
	b := baseToken{}

	if b.WaitTimeout(time.Second) {
		t.Fatal("Should have failed")
	}
}
